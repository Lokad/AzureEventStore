using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace Lokad.AzureEventStore.State
{
    /// <summary> An immutable key-value map. </summary>
    public struct SnapshotMap<TK,TV> : IReadOnlyDictionary<TK,TV>
    {
        /// <summary> Creates a new thread-unsafe empty table. </summary>
        public static SnapshotMap<TK, TV> Empty => new SnapshotMap<TK, TV>(null);

        /// <summary> Set the value for a specific key. </summary>        
        public SnapshotMap<TK, TV> SetItem(TK key, TV value) =>
            new SnapshotMap<TK, TV>((_root ?? new Node()).SetItem(key, value));

        /// <summary> Add a new item for a key that does not already exist. </summary>
        public SnapshotMap<TK, TV> Add(TK key, TV value) =>
            new SnapshotMap<TK, TV>((_root ?? new Node()).SetItem(key, value, isAdd: true));

        /// <see cref="IReadOnlyDictionary{T,T}.this"/>
        public TV this[TK k]
        {
            get
            {
                TV value;
                if (TryGetValue(k, out value)) return value;
                throw new ArgumentOutOfRangeException(nameof(k), "No value exists for this key.");
            }
        }
        /// <see cref="IReadOnlyDictionary{T,T}.Keys"/>
        public IEnumerable<TK> Keys
        {
            get
            {
                foreach (var kv in this)
                    yield return kv.Key;
            }
        }

        /// <see cref="IReadOnlyDictionary{T,T}.Values"/>
        public IEnumerable<TV> Values
        {
            get
            {
                foreach (var kv in this)
                    yield return kv.Value;
            }
        }

        /// <see cref="IReadOnlyDictionary{T,T}.ContainsKey"/>
        public bool ContainsKey(TK key)
        {
            TV ignore;
            return TryGetValue(key, out ignore);
        }

        /// <see cref="IReadOnlyDictionary{T,T}.TryGetValue"/>
        public bool TryGetValue(TK key, out TV value)
        {
            if (_root == null)
            {
                value = default(TV);
                return false;
            }

            return _root.TryGetValue(key, out value);
        }

        /// <see cref="IReadOnlyDictionary{T,T}.Count"/>
        public int Count => _root?.Count ?? 0;

        /// <summary>
        /// Mark this table, and all tables created from it through modification,
        /// as eligible for multi-threaded access.
        /// </summary>
        public SnapshotMap<TK, TV> MakeMultiThreaded()
        {
            var node = _root ?? new Node();
            node.MakeMultiThreaded();
            return new SnapshotMap<TK, TV>(node);
        }

        #region Private implementation

        private readonly Node _root;

        private SnapshotMap(Node root)
        {
            _root = root;
        }

        /// <summary> An entry in the hash table. </summary>
        private struct Entry
        {
            /// <summary> The hash code of this entry. </summary>
            public int Hash { get; set; }

            /// <summary> The pointer to the next entry in this bucket. </summary>
            public int Next { get; set; }

            /// <summary> The key of this entry. </summary>
            public TK Key { get; set; }

            /// <summary> The value of this entry. </summary>
            public TV Value { get; set; }
        }

        /// <summary> The underlying data object for the tree of changes. </summary>
        private sealed class Data
        {
            /// <summary> The dictionary entries. </summary>
            public readonly Entry[] Entries;

            /// <summary> The first list element in each bucket. </summary>
            public readonly int[] Buckets;

            /// <summary> The first element in the free-entry-list. </summary>
            public int Free;

            /// <summary> The number of elements in this dictionary. </summary>
            public int Count;

            /// <summary> A lock-free signal. </summary>
            /// <remarks>
            ///     Becomes odd when the tree of changes is being modified.
            ///     Takes on a new value every time the tree of changes is modified.            
            /// </remarks>
            public int FastLock;

            /// <summary> A lock used to restrict multi-threaded access to writes. </summary>
            /// <remarks> NULL if access is single-threaded. </remarks>
            public SemaphoreSlim HardLock;

            public Data(int entryCount, int bucketCount)
            {
                var entries = Entries = new Entry[entryCount];
                for (var i = 0; i < entryCount; ++i)
                    entries[i].Next = i - 1;

                var buckets = Buckets = new int[bucketCount];
                for (var i = 0; i < bucketCount; ++i)
                    buckets[i] = -1;

                Count = 0;
                Free = entryCount - 1;
            }

            /// <summary> Find the entry that matches the provided key, -1 if none. </summary>
            public int FindEntry(int hash, TK key)
            {
                var i = Buckets[hash % Buckets.Length];
                while (i >= 0)
                {
                    var entry = Entries[i];
                    if (entry.Hash == hash && entry.Key.Equals(key)) return i;
                    i = entry.Next;
                }

                return -1;
            }

            /// <summary> Create a new entry. </summary>
            public void CreateEntry(int hash, TK key, TV value)
            {
                var free = Free;

                Debug.Assert(free != -1);

                Free = Entries[free].Next;
                Entries[free] = new Entry
                {
                    Hash = hash,
                    Key = key,
                    Next = Buckets[hash % Buckets.Length],
                    Value = value
                };

                Buckets[hash % Buckets.Length] = free;

                ++Count;
            }

            /// <summary> Remove the specified entry. </summary>
            public void RemoveEntry(int entry)
            {
                Entries[entry] = new Entry
                {
                    Hash = 0,
                    Key = default(TK),
                    Value = default(TV),
                    Next = Free
                };

                --Count;

                Free = entry;
            }

            /// <summary> Reads the next entry. </summary>
            public int NextEntry(int entry, out KeyValuePair<TK, TV> pair)
            {
                var bucket = 0;
                if (entry >= 0)
                {
                    // If caller provided entry, then move to the next one
                    // (and be prepared to start from the NEXT bucket if there isn't)
                    bucket = (Entries[entry].Hash % Buckets.Length) + 1;
                    entry = Entries[entry].Next;
                }

                if (entry < 0)
                {
                    // Still no entry ? Search through buckets.
                    while (entry < 0 && bucket < Buckets.Length) entry = Buckets[bucket++];
                    if (entry < 0)
                    {
                        // No entry found in any bucket, so we're done.
                        pair = default(KeyValuePair<TK, TV>);
                        return -1;
                    }
                }

                // We have an entry, so return it.
                pair = new KeyValuePair<TK, TV>(Entries[entry].Key, Entries[entry].Value);
                return entry;
            }
        }

        /// <summary> The internal representation is a list and tree of changes. </summary>
        /// <remarks>
        ///     Each node is either a reference to a parent and an individual change
        ///     or is the root and contains the list of values.
        /// </remarks>
        private sealed class Node
        {
            public Node() { Data = new Data(4, 4); }

            private Node(Data data) { Data = data; }

            /// <summary> The number of keys. </summary>
            public int Count
            {
                get
                {
                    // Counting cannot be done unless we're at the root. First, try to acquire 
                    // a fast lock...

                    Interlocked.MemoryBarrier();
                    var oldToken = Data.FastLock;
                    Interlocked.MemoryBarrier();

                    // An even lock token means no structure update is underway
                    if (oldToken % 2 == 0)
                    {
                        if (Parent == null)
                        {
                            var count = Data.Count;

                            Interlocked.MemoryBarrier();
                            var newToken = Data.FastLock;
                            Interlocked.MemoryBarrier();

                            // No change happened, so the count we extracted is good.
                            if (oldToken == newToken) return count;
                        }
                    }

                    // We couldn't access the value with a fast lock...

                    Data.HardLock?.Wait();
                    try
                    {
                        Flatten();
                        return Data.Count;
                    }
                    finally
                    {
                        Data.HardLock?.Release();
                    }
                }
            }

            /// <summary> Bind a value to a key, throw if adding and the key already exists. </summary>
            public Node SetItem(TK key, TV value, bool isAdd = false)
            {
                var node = new Node(Data);

                var hash = key.GetHashCode();

                var lockAcquired = Data.HardLock == null || Data.HardLock.Wait(0);
                if (lockAcquired || isAdd)
                {
                    // 'isAdd' requires that we check whether the key already exists, so we 
                    // accept the performance hit of waiting for the lock. 
                    if (!lockAcquired) Data.HardLock.Wait();

                    // We have a lock, so just mutate the underlying dictionary
                    try
                    {
                        Flatten();

                        var entry = Data.FindEntry(hash, key);
                        var exists = entry >= 0;
                        if (exists && isAdd)
                            throw new InvalidOperationException("Cannot add, key already exists in dictionary");

                        // Not enough data...
                        if (!exists && Data.Free < 0)
                            return Enlarge().SetItem(key, value, isAdd);

                        var current = exists ? Data.Entries[entry].Value : default(TV);

                        Interlocked.Increment(ref Data.FastLock);

                        Parent = node;
                        SetKey = key;
                        SetValue = current;
                        UnsetValue = !exists;

                        if (exists)
                            Data.Entries[entry].Value = value;
                        else
                            Data.CreateEntry(hash, key, value);

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
                    node.Hash = hash;
                    node.SetKey = key;
                    node.SetValue = value;
                }

                return node;
            }

            /// <summary> Return a node with a higher-capacity data. </summary>
            /// <remarks> Should only be called on a hard-locked, flattened node. </remarks>
            private Node Enlarge()
            {
                var data = new Data(Data.Entries.Length * 2, Data.Buckets.Length * 2);
                foreach (var first in Data.Buckets)
                {
                    var e = first;
                    while (e >= 0)
                    {
                        data.CreateEntry(Data.Entries[e].Hash, Data.Entries[e].Key, Data.Entries[e].Value);
                        e = Data.Entries[e].Next;
                    }
                }

                var node = new Node(data);
                if (Data.HardLock != null) node.MakeMultiThreaded();
                return node;
            }

            private TK SetKey { get; set; }
            private TV SetValue { get; set; }
            private Node Parent { get; set; }
            private Data Data { get; }
            private int Hash { get; set; }
            private bool UnsetValue { get; set; }

            /// <summary>
            /// Allowed to dive through this many nodes before flattening becomes
            /// necessary. 
            /// </summary>
            private const int MaxSearchDepth = 5;

            public bool TryGetValue(TK key, out TV value)
            {
                var hash = key.GetHashCode();

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
                    var isset = false;
                    var found = false;

                    value = default(TV);

                    for (var n = 0; !found && node.Parent != null && n < MaxSearchDepth; ++n)
                    {
                        if (node.Hash == hash && node.SetKey.Equals(key))
                        {
                            value = node.SetValue;
                            isset = !node.UnsetValue;
                            found = true;
                        }

                        node = node.Parent;
                    }

                    if (!found && node.Parent == null)
                    {
                        found = true;
                        var entry = Data.FindEntry(hash, key);
                        isset = entry >= 0;
                        if (isset) value = Data.Entries[entry].Value;
                    }

                    if (found)
                    {
                        Interlocked.MemoryBarrier();
                        var newToken = Data.FastLock;
                        Interlocked.MemoryBarrier();

                        // No changes occurred while we were reading, so just return the value.
                        if (oldToken == newToken) return isset;
                    }
                }

                // Our optimistic attempt failed (either because someone tried a 
                // write, or the value was too deep). 
                Data.HardLock?.Wait();
                try
                {
                    Flatten();

                    var entry = Data.FindEntry(hash, key);
                    value = entry >= 0 ? Data.Entries[entry].Value : default(TV);
                    return entry >= 0;
                }
                finally
                {
                    Data.HardLock?.Release();
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

                var entry = Data.FindEntry(Hash, SetKey);

                Parent.SetKey = SetKey;
                Parent.Hash = Hash;
                Parent.SetValue = entry >= 0 ? Data.Entries[entry].Value : default(TV);
                Parent.UnsetValue = entry < 0;

                if (entry < 0)
                {
                    if (!UnsetValue)
                        Data.CreateEntry(Hash, SetKey, SetValue);
                }
                else
                {
                    if (UnsetValue)
                        Data.RemoveEntry(entry);
                    else
                        Data.Entries[entry].Value = SetValue;
                }

                Parent = null;
            }

            /// <summary> Allow multiple threads to access the object.</summary>
            /// <remarks> Decreases performance. </remarks>
            public void MakeMultiThreaded()
            {
                Data.HardLock = new SemaphoreSlim(1);
            }

            /// <summary> Steps to the next entry (or first, if <paramref name="from"/> is -1). </summary>
            public int Next(int from, out KeyValuePair<TK, TV> pair)
            {
                // Iteration cannot be done unless we're at the root. First, try to acquire 
                // a fast lock...

                Interlocked.MemoryBarrier();
                var oldToken = Data.FastLock;
                Interlocked.MemoryBarrier();

                // An even lock token means no structure update is underway
                if (oldToken % 2 == 0)
                {
                    if (Parent == null)
                    {
                        var entry = Data.NextEntry(from, out pair);

                        Interlocked.MemoryBarrier();
                        var newToken = Data.FastLock;
                        Interlocked.MemoryBarrier();

                        // No change happened, so the element we extracted is good.
                        if (oldToken == newToken) return entry;
                    }
                }

                // We couldn't access the value with a fast lock...

                Data.HardLock?.Wait();
                try
                {
                    Flatten();
                    return Data.NextEntry(from, out pair);
                }
                finally
                {
                    Data.HardLock?.Release();
                }

            }
        }

        #endregion

        public IEnumerator<KeyValuePair<TK, TV>> GetEnumerator()
        {
            if (_root == null) yield break;

            KeyValuePair<TK, TV> pair;

            var entry = _root.Next(-1, out pair);
            while (entry >= 0)
            {
                yield return pair;
                entry = _root.Next(entry, out pair);
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
