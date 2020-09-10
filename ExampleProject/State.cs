using System;
using System.Collections.Immutable;
using Lokad.LargeImmutable;
using MessagePack;
using MessagePack.ImmutableCollection;
using MessagePack.Resolvers;

namespace ExampleProject
{
    /// <summary> 
    ///     The immutable state of the application: a library of documents and an index 
    ///     to find documents containing a value.
    /// </summary>
    public sealed class State
    {
        /// <summary> A document. </summary>
        
        [MessagePackObject]
        public sealed class Document
        {
            public Document(int id, string path, string contents)
            {
                Id = id;
                Path = path ?? throw new ArgumentNullException(nameof(path));
                Contents = contents ?? throw new ArgumentNullException(nameof(contents));
            }

            /// <summary>
            ///     The identifier of the document (its position in the documents
            ///     array).
            /// </summary>
            [Key(0)]
            public int Id { get; }

            /// <summary> The path from which the document was imported. </summary>
            [Key(1)]
            public string Path { get; }

            /// <summary> The contents of the document. </summary>
            [Key(2)]
            public string Contents { get; }
        }

        public State(
            ImmutableDictionary<string, int> index,
            LargeImmutableList<ImmutableList<int>> documentLists,
            LargeImmutableList<Document> documents)
        {
            Index = index ?? throw new ArgumentNullException(nameof(index));
            DocumentLists = documentLists ?? throw new ArgumentNullException(nameof(documentLists));
            Documents = documents ?? throw new ArgumentNullException(nameof(documents));
        }

        /// <summary> 
        ///     For each word, the position (in <see cref="DocumentLists"/>) of the list of all documents 
        ///     where it appears, case-insensitive.
        /// </summary>
        public ImmutableDictionary<string,int> Index { get; }

        /// <summary> The table of document lists. </summary>
        /// <remarks>
        ///     Documents are indexed by their position in <see cref="Documents"/>.
        /// </remarks>
        public LargeImmutableList<ImmutableList<int>> DocumentLists { get; }

        /// <summary> All known documents. </summary>
        public LargeImmutableList<Document> Documents { get; } 

        /// <summary>
        ///     The MessagePack options to use to serialize and deserialize the contents 
        ///     of the LargeImmutableList tables.
        /// </summary>
        public static MessagePackSerializerOptions MessagePackOptions { get; } = 
            MessagePackSerializerOptions.Standard.WithResolver(
                CompositeResolver.Create(
                    ImmutableCollectionResolver.Instance,
                    StandardResolver.Instance));
    }
}
