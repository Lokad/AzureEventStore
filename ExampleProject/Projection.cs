using System;
using System.Collections.Immutable;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Projections;
using ExampleProject.Events;
using Lokad.LargeImmutable;
using System.Collections.Generic;
using Lokad.AzureEventStore.Cache;
using MessagePack;
using System.Text.RegularExpressions;
using Lokad.AzureEventStore;

namespace ExampleProject
{
    public sealed class Projection : IProjection<IEvent, State>
    {
        public State Initial(StateCreationContext stateCreationContext) => new State(
            ImmutableDictionary<string, int>.Empty,
            LargeImmutableList<ImmutableList<int>>.Empty(State.MessagePackOptions),
            LargeImmutableList<State.Document>.Empty(State.MessagePackOptions));

        /// <summary> Update the state by applying an event. </summary>
        public State Apply(uint sequence, IEvent e, State previous)
        {
            // In a real-life project, this would likely have been implemented using
            // a visitor pattern (so that, when adding event types, the compiler can
            // help us detect all cases where these new events need to be handled).

            if (e is DocumentAdded d)
            {
                if (d.Id != previous.Documents.Count)
                    throw new InvalidDataException();

                // Add the new document to the table of documents
                var state = new State(
                    previous.Index, 
                    previous.DocumentLists, 
                    previous.Documents.Add(
                        new State.Document(d.Id, d.Path, d.Contents)));

                // Index all words in the document
                foreach (var word in WordsOf(d.Contents))
                {
                    if (state.Index.TryGetValue(word, out var list))
                    {
                        // Word exists in the index: update the documents list.
                        state = new State(
                            state.Index,
                            state.DocumentLists.SetItem(
                                list, 
                                state.DocumentLists[list].Add(d.Id)),
                            state.Documents);
                    }
                    else
                    {
                        // Word does not exist in the index: add to index, and 
                        // create new documents list.
                        state = new State(
                            state.Index.Add(word, state.DocumentLists.Count),
                            state.DocumentLists.Add(
                                ImmutableList<int>.Empty.Add(d.Id)),
                            state.Documents);
                    }
                }

                return state;
            }

            if (e is DocumentRemoved r)
            {
                if (previous.Documents.Count <= r.Id || r.Id < 0)
                    throw new InvalidDataException();

                if (previous.Documents[r.Id] == null)
                    throw new InvalidDataException();

                var doc = previous.Documents[r.Id];

                // Remove the document
                var state = new State(
                    previous.Index,
                    previous.DocumentLists,
                    previous.Documents.SetItem(r.Id, null));

                // Remove the document id from the document lists of all 
                // indexed words (leaving empty document lists is acceptable).
                foreach (var word in WordsOf(doc.Contents))
                {
                    var list = state.Index[word];
                    state = new State(
                        state.Index,
                        state.DocumentLists.SetItem(
                            list,
                            state.DocumentLists[list].Remove(r.Id)),
                        state.Documents);
                }

                return state;
            }

            throw new ArgumentOutOfRangeException(nameof(e), "Unknown event type " + e.GetType());
        }

        /// <summary> Cut a document into distinct words. </summary>
        private static IEnumerable<string> WordsOf(string words)
        {
            var set = new HashSet<string>();

            foreach (var w in new Regex("\\W").Split(words))
            {
                if (string.IsNullOrWhiteSpace(w)) continue;

                set.Add(w.ToLowerInvariant());
            }

            return set;
        }

        /// <summary> Load the state from a stream. </summary>
        public async Task<State> TryLoadAsync(Stream source, CancellationToken cancel)
        {
            // Discover if the source can be memory-mapped, or copy it to an in-memory buffer.
            var stream = await source.AsBigMemoryStream(cancel);

            var index = await MessagePackSerializer.DeserializeAsync<ImmutableDictionary<string, int>>(
                stream, State.MessagePackOptions);

            // LargeImmutableList uses memory-mapping to avoid reloading the entire tables
            // from the disk.
            var documentLists = LargeImmutableList<ImmutableList<int>>.Load(stream, State.MessagePackOptions);
            var documents = LargeImmutableList<State.Document>.Load(stream, State.MessagePackOptions);

            return new State(index, documentLists, documents);
        }

        /// <summary> Save the state to a stream. </summary>
        public async Task<bool> TrySaveAsync(Stream destination, State state, CancellationToken cancel)
        {
            await MessagePackSerializer.SerializeAsync(destination, state.Index, State.MessagePackOptions);
            state.DocumentLists.Save(destination);
            state.Documents.Save(destination);

            return true;
        }

        public Task<RestoredState<State>> TryRestoreAsync(StateCreationContext stateCreationContext, CancellationToken cancel = default)
        {
            return Task.FromResult<RestoredState<State>>(null);
        }

        public Task CommitAsync(State state, uint sequence, CancellationToken cancel = default)
        {
            return Task.CompletedTask;
        }

        public Task<State> UpkeepAsync(StateUpkeepContext stateUpkeepContext, State state, CancellationToken cancel = default)
        {
            return Task.FromResult(state);
        }

        /// <see cref="IProjection{TEvent}.FullName"/>
        public string FullName => "State";

        /// <see cref="IProjection{TEvent}.State"/>
        Type IProjection<IEvent>.State => typeof(State);
    }
}
