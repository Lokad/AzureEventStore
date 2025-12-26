using System;
using System.Collections.Immutable;
using System.IO;
using MessagePack;
using MessagePack.ImmutableCollection;
using MessagePack.Resolvers;

namespace ExampleProject;

/// <summary> 
///     The immutable state of the application: a library of documents and an index 
///     to find documents containing a value.
/// </summary>
public sealed class State(
    ImmutableDictionary<string, int> index,
    ImmutableList<ImmutableList<int>> documentLists,
    ImmutableList<State.Document> documents)
{
    /// <summary> A document. </summary>
        
    [MessagePackObject]
    public sealed class Document(int id, string path, string contents)
    {
        /// <summary>
        ///     The identifier of the document (its position in the documents
        ///     array).
        /// </summary>
        [Key(0)]
        public int Id { get; } = id;

        /// <summary> The path from which the document was imported. </summary>
        [Key(1)]
        public string Path { get; } = path ?? throw new ArgumentNullException(nameof(path));

        /// <summary> The contents of the document. </summary>
        [Key(2)]
        public string Contents { get; } = contents ?? throw new ArgumentNullException(nameof(contents));
    }

    /// <summary> 
    ///     For each word, the position (in <see cref="DocumentLists"/>) of the list of all documents 
    ///     where it appears, case-insensitive.
    /// </summary>
    public ImmutableDictionary<string,int> Index { get; } = index ?? throw new ArgumentNullException(nameof(index));

    /// <summary> The table of document lists. </summary>
    /// <remarks>
    ///     Documents are indexed by their position in <see cref="Documents"/>.
    /// </remarks>
    public ImmutableList<ImmutableList<int>> DocumentLists { get; } = documentLists ?? throw new ArgumentNullException(nameof(documentLists));

    /// <summary> All known documents. </summary>
    public ImmutableList<Document> Documents { get; } = documents ?? throw new ArgumentNullException(nameof(documents));

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