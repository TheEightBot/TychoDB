namespace TychoDB;

/// <summary>
/// Selects a preset of SQLite PRAGMA tuning appropriate for the target device
/// class. Individual values (page cache size, mmap size) can still be overridden
/// on the <see cref="Tycho"/> constructor.
/// </summary>
public enum TychoPerformanceProfile
{
    /// <summary>
    /// Conservative memory use for phones/tablets: a small page cache, a modest
    /// memory-map, and frequent WAL checkpoints to keep the WAL file small. This
    /// is the default.
    /// </summary>
    Mobile = 0,

    /// <summary>
    /// Throughput-oriented settings for desktops/servers: a large page cache, a
    /// large memory-map for fast reads, and less frequent WAL checkpoints.
    /// </summary>
    Desktop = 1,
}
