using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Projections
{
    /// <summary>
    /// Uses one or more <see cref="ReifiedProjection{T,T}"/> instances
    /// to keep a complex state up-to-date with events.
    /// </summary>
    /// <typeparam name="TEvent"> Events processed to update the state. </typeparam>
    /// <typeparam name="TState"> The type of the state. Immutable. </typeparam>
    internal sealed class ReifiedProjectionGroup<TEvent, TState> : IReifiedProjection<TEvent, TState> where TState : class
    {
        /// <summary>
        /// All the reified projections involving in building the state for
        /// this group.
        /// </summary>
        private readonly IReifiedProjection<TEvent>[] _reifiedProjections;

        /// <summary> Cached state, computed lazily. </summary>        
        private Lazy<TState> _current;

        /// <summary>
        /// Reifies a group of projections as a <typeparamref name="TState"/> object.
        /// </summary>
        public ReifiedProjectionGroup(IEnumerable<IProjection<TEvent>> projections, IProjectionCacheProvider cacheProvider = null, ILogAdapter log = null)
        {
            // Dirty reflection work to reify the individual projections and store their
            // type parameter.
            var reifiedByType = new Dictionary<Type, IReifiedProjection<TEvent>>();
            foreach (var p in projections)
            {
                if (reifiedByType.ContainsKey(p.State))
                {
                    log?.Error($"Found multiple projections for type {p.State}.");
                    throw new ArgumentException($"Multiple projections for type '{p.State}'", nameof(projections));
                }

                var reifiedProjectionType = typeof (ReifiedProjection<,>)
                    .MakeGenericType(typeof (TEvent), p.State);

                var projectionType = typeof (IProjection<,>)
                    .MakeGenericType(typeof (TEvent), p.State);

                var constructor = reifiedProjectionType
                    .GetTypeInfo()
                    .GetConstructor(new []{ projectionType, typeof(IProjectionCacheProvider), typeof(ILogAdapter) });

                if (constructor == null)
                    throw new Exception("No constructor found for '" + reifiedProjectionType + "'");

                IReifiedProjection<TEvent> reified;

                try
                {
                    reified = (IReifiedProjection<TEvent>) constructor.Invoke(new object[] {p, cacheProvider, log});
                }
                catch (TargetInvocationException e)
                {
                    log?.Error($"When creating reified projection for type {p.State}.", e.InnerException);
                    throw e.InnerException;
                }

                reifiedByType.Add(p.State, reified);
            }

            // Easy case: a projection outputs the state type directly
            IReifiedProjection<TEvent> direct;
            if (reifiedByType.TryGetValue(typeof (TState), out direct))
            {
                _reifiedProjections = new[] {direct};
                var directCast = (ReifiedProjection<TEvent, TState>) direct;

                _refresh = () => directCast.Current;
                
                InvalidateCurrent();
                return;
            }

            // Hard case: state is made from the results of multiple independent 
            // projections. Need to write code to combine them.
            var posByType = new Dictionary<Type, int>();
            var reifiedByPos = new List<IReifiedProjection<TEvent>>();

            var stateConstructor = typeof (TState)
                .GetTypeInfo()
                .GetConstructors()
                .FirstOrDefault(c =>
                {
                    var p = c.GetParameters();
                    if (p.Length == 0) return false;
                    return p.All(info => reifiedByType.ContainsKey(info.ParameterType));
                });

            if (stateConstructor == null)
                throw new ArgumentException($"No compatible constructor found in '{typeof(TState)}'", nameof(projections));

            // Build an expression that will extract the state components from "array" and
            // pass them to the constructor.
            var parameters = stateConstructor.GetParameters();
            var array = Expression.Parameter(typeof (IReifiedProjection<TEvent>[]));
            var arguments = new List<Expression>();
            foreach (var p in parameters)
            {
                int pos;
                if (!posByType.TryGetValue(p.ParameterType, out pos))
                {
                    pos = posByType.Count;
                    posByType.Add(p.ParameterType, pos);
                    reifiedByPos.Add(reifiedByType[p.ParameterType]);
                }

                var argReified = Expression.Convert(
                    Expression.ArrayIndex(array, Expression.Constant(pos)),
                    typeof (ReifiedProjection<,>).MakeGenericType(typeof (TEvent), p.ParameterType));
                
                arguments.Add(Expression.Property(argReified, "Current"));
            }

            var lambda = Expression.Lambda<Func<IReifiedProjection<TEvent>[], TState>>(
                Expression.New(stateConstructor, arguments), array);

            var extract = lambda.Compile();
            var reifiedProjections = _reifiedProjections = reifiedByPos.ToArray();

            _refresh = () => extract(reifiedProjections);

            InvalidateCurrent();
        } 

        /// <summary> The current state of the projections. </summary>
        public TState Current => _current.Value;

        public async Task<bool> TrySaveAsync(CancellationToken cancel = default(CancellationToken))
        {
            var all = await Task.WhenAll(
                _reifiedProjections.Select(p => p.TrySaveAsync(cancel)));

            return all.All(b => b);
        }

        public async Task TryLoadAsync(CancellationToken cancel = default(CancellationToken))
        {
            await Task.WhenAll(_reifiedProjections.Select(p => p.TryLoadAsync(cancel)));
            Sequence = _reifiedProjections.Min(p => p.Sequence);
        }

        public void SetPossiblyInconsistent()
        {
            foreach (var p in _reifiedProjections) p.SetPossiblyInconsistent();
        }

        public void Apply(uint seq, TEvent e)
        {
            if (seq <= Sequence)
                throw new ArgumentException("Sequence number too early.");

            Sequence = seq;

            List<Exception> exceptions = null;
            foreach (var p in _reifiedProjections)
            {
                if (seq > p.Sequence)
                {
                    try
                    {
                        p.Apply(seq, e);
                    }
                    catch (Exception ex)
                    {
                        SetPossiblyInconsistent();
                        if (exceptions == null) exceptions = new List<Exception>();
                        exceptions.Add(ex);
                    }
                }
            }

            if (exceptions != null) throw new AggregateException(exceptions);

            InvalidateCurrent();
        }

        /// <summary> Passing events through every projection without invalidation or changes in projection. </summary>
        public void TryApply(uint seq, IReadOnlyList<TEvent> events)
        {
            foreach (var p in _reifiedProjections)
            {
                p.TryApply(seq, events);
            }
        }

        public void Reset()
        {
            foreach (var p in _reifiedProjections) p.Reset();
            Sequence = 0;
            InvalidateCurrent();
        }

        /// <summary> Marks <see cref="Current"/> as outdated and requiring re-evaluation. </summary>
        private void InvalidateCurrent()
        {
            _current = new Lazy<TState>(_refresh, LazyThreadSafetyMode.PublicationOnly);
        }

        /// <summary> Construct the state from the sub-projections. </summary>
        private readonly Func<TState> _refresh;

        public uint Sequence { get; private set; }

        /// <see cref="IReifiedProjection{T}.Name"/>
        public string Name { get { return "{" + string.Join(",", _reifiedProjections.Select(p => p.Name)) + "}"; } }
    }
}
