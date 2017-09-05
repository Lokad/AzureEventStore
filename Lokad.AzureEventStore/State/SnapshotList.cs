using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Threading;

namespace Lokad.AzureEventStore.State
{
    /// <summary> An immutable table of values of the specified type. </summary>
    public struct SnapshotList<T> : IReadOnlyList<T>
    {
        /// <summary> Creates a new thread-unsafe empty table. </summary>
        public static SnapshotList<T> Empty => default(SnapshotList<T>);

        /// <see cref="IReadOnlyList{T}.Count"/>
        public int Count { get; }

        /// <summary> Set the item at a specific location. </summary>        
        public SnapshotList<T> SetItem(int i, T value)
        {
            if (i < 0 || i >= Count)
                throw new IndexOutOfRangeException();

            return new SnapshotList<T>(_root.SetItem(i, value), Count);
        }

        /// <summary> Add a new item at the end. </summary>
        [Pure] public SnapshotList<T> Add(T value) =>
            new SnapshotList<T>((_root ?? new Node()).SetItem(Count, value), Count + 1);

        /// <summary> Take ownership of the array and use as base data. </summary>        
        public SnapshotList(T[] array)
        {
            if (array.Length == 0)
            {
                Count = 0;
                _root = null;
                return;
            }

            Count = array.Length;
            _root = new Node(array);
        }

        /// <summary> Remove the last item in the list. </summary>
        public SnapshotList<T> Remove()
        {
            if (Count == 0)
                throw new InvalidOperationException("Cannot remove from empty list.");

            if (Count == 1)
            {
                // When reverting to an empty list, spawn a brand new data array so that we
                // don't have to interact with the locks on previous iterations. 
                var ret = Empty;
                if (_root?.IsMultiThreaded ?? false) ret = ret.MakeMultiThreaded();
                return ret;
            }

            return new SnapshotList<T>(_root, Count - 1);
        }

        /// <see cref="IReadOnlyList{T}.this"/>
        public T this[int i]
        {
            get
            {
                if (i < 0 || i >= Count)
                    throw new IndexOutOfRangeException();

                return _root[i];
            }
        }

        /// <summary>
        /// Mark this table, and all tables created from it through modification,
        /// as eligible for multi-threaded access.
        /// </summary>
        public SnapshotList<T> MakeMultiThreaded()
        {
            var node = _root ?? new Node();
            node.MakeMultiThreaded();
            return new SnapshotList<T>(node, Count);
        }

        #region Private implementation

        private readonly Node _root;

        private SnapshotList(Node root, int count)
        {
            Debug.Assert(root != null);

            _root = root;
            Count = count;
        }

        /// <summary> The underlying data object for the tree of changes. </summary>
        private sealed class Data
        {
            /// <summary> An array of values. The actual list only uses a prefix. </summary>
            public readonly T[] Values;

            /// <summary> A lock-free signal. </summary>
            /// <remarks>
            ///     Becomes odd when the tree of changes is being modified.
            ///     Takes on a new value every time the tree of changes is modified.            
            /// </remarks>
            public int FastLock;

            /// <summary> A lock used to restrict multi-threaded access to writes. </summary>
            /// <remarks> NULL if access is single-threaded. </remarks>
            public SemaphoreSlim HardLock;

            public Data(T[] values)
            {
                Values = values;
            }
        }

        /// <summary> The internal representation is a list and tree of changes. </summary>
        /// <remarks>
        ///     Each node is either a reference to a parent and an individual change
        ///     or is the root and contains the list of values.
        /// </remarks>
        private sealed class Node
        {
            public Node() { Data = new Data(new T[4]); }

            public Node(T[] array) { Data = new Data(array); }

            private Node(Data data) { Data = data; }

            private Node(Data data, bool allowMultithread) : this(data)
            {
                if (allowMultithread) MakeMultiThreaded();
            }

            /// <summary> Whether this node supports multi-threaded access. </summary>
            public bool IsMultiThreaded => Data.HardLock != null;

            public Node SetItem(int i, T value)
            {
                // New value fits in existing array: create a new root node
                // and point this one towards it.
                if (i < Data.Values.Length)
                {
                    var node = new Node(Data);

                    if (Data.HardLock == null || Data.HardLock.Wait(0))
                    {
                        // The lock was available, so just mutate the underlying array
                        try
                        {
                            Flatten();

                            Interlocked.Increment(ref Data.FastLock);
                            Parent = node;
                            SetKey = i;
                            SetValue = Data.Values[i];
                            Data.Values[i] = value;
                            Interlocked.Increment(ref Data.FastLock);
                        }
                        finally
                        {
                            Data.HardLock?.Release();
                        }
                    }
                    else
                    {
                        // The lock was unavailable, so create a pending modification
                        // without touching the source.
                        node.Parent = this;
                        node.SetKey = i;
                        node.SetValue = value;
                    }

                    return node;
                }

                // New value is outside existing array: enlarge, then create a
                // new independent node.
                var root = new T[Data.Values.Length * 2];

                Data.HardLock?.Wait();
                try
                {
                    Flatten();
                    Array.Copy(Data.Values, root, Data.Values.Length);
                }
                finally
                {
                    Data.HardLock?.Release();
                }

                root[i] = value;
                return new Node(new Data(root), Data.HardLock != null);
            }

            private int SetKey { get; set; }
            private T SetValue { get; set; }
            private Node Parent { get; set; }
            private Data Data { get; }

            /// <summary>
            /// Allowed to dive through this many nodes before flattening becomes
            /// necessary. 
            /// </summary>
            private const int MaxSearchDepth = 5;

            public T this[int i]
            {
                get
                {
                    // First, try the lock-free approach. It avoids locks when no writes are
                    // underway and all readers (possibly more than one) are hitting close to the
                    // current root (thus requiring no flattening).

                    Interlocked.MemoryBarrier();
                    var oldToken = Data.FastLock;
                    Interlocked.MemoryBarrier();

                    // An even lock token means no structure update is underway
                    if (oldToken % 2 == 0)
                    {
                        // Are we on the root, or close enough ?
                        var node = this;
                        var found = false;
                        var value = default(T);

                        for (var n = 0; !found && node.Parent != null && n < MaxSearchDepth; ++n)
                        {
                            if (node.SetKey == i)
                            {
                                value = node.SetValue;
                                found = true;
                            }

                            node = node.Parent;
                        }

                        if (!found && node.Parent == null)
                        {
                            found = true;
                            value = Data.Values[i];
                        }

                        if (found)
                        {
                            Interlocked.MemoryBarrier();
                            var newToken = Data.FastLock;
                            Interlocked.MemoryBarrier();

                            // No changes occurred while we were reading, so just return the value.
                            if (oldToken == newToken) return value;
                        }
                    }

                    // Our optimistic attempt failed (either because someone tried a 
                    // write, or the value was too deep).
                    Data.HardLock?.Wait();
                    try
                    {
                        Flatten();
                        return Data.Values[i];
                    }
                    finally
                    {
                        Data.HardLock?.Release();
                    }
                }
            }

            private void Flatten()
            {
                // Early-out if no flattening is required
                if (Parent == null) return;

                // We need to change data, so increment the lock. This should make the
                // lock an odd number, signifying "we're changing stuff".
                Interlocked.Increment(ref Data.FastLock);
                try
                {
                    // Try short distances first, as they are more frequent     
                    if (Parent.Parent == null)
                    {
                        Swap();
                        return;
                    }

                    if (Parent.Parent.Parent == null)
                    {
                        Parent.Swap();
                        Swap();
                        return;
                    }

                    if (Parent.Parent.Parent.Parent == null)
                    {
                        Parent.Parent.Swap();
                        Parent.Swap();
                        Swap();
                        return;
                    }

                    // The root is further away, so dig in
                    var stack = new Stack<Node>();
                    var node = this;

                    while (node.Parent != null)
                    {
                        stack.Push(node);
                        node = node.Parent;
                    }

                    while (stack.Count > 0)
                    {
                        node = stack.Pop();
                        node.Swap();
                    }
                }
                finally
                {
                    // Increment the lock again, making it even ("we're not changing stuff")
                    // and invalidating any reads performed during the interval.
                    Interlocked.Increment(ref Data.FastLock);
                }
            }

            private void Swap()
            {
                Parent.Parent = this;

                Parent.SetKey = SetKey;
                Parent.SetValue = Data.Values[SetKey];
                Data.Values[SetKey] = SetValue;

                Parent = null;
            }

            /// <summary> Allow multiple threads to access the object.</summary>
            /// <remarks> Decreases performance. </remarks>
            public void MakeMultiThreaded()
            {
                Data.HardLock = new SemaphoreSlim(1);
            }
        }

        #endregion

        /// <see cref="IReadOnlyList{T}.GetEnumerator"/>
        public IEnumerator<T> GetEnumerator()
        {
            for (var i = 0; i < Count; ++i)
                yield return this[i];
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
