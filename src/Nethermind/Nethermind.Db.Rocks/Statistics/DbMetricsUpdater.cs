// SPDX-FileCopyrightText: 2022 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Collections.Generic;
using System.Drawing.Printing;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using Nethermind.Db.Rocks.Config;
using Nethermind.Logging;
using RocksDbSharp;

namespace Nethermind.Db.Rocks.Statistics;

public partial class DbMetricsUpdater
{
    private readonly string _dbName;
    private readonly DbOptions _dbOptions;
    private readonly RocksDb _db;
    private readonly IDbConfig _dbConfig;
    private readonly ILogger _logger;
    private readonly ColumnFamilyHandle? _columnFamilyHandle;
    private Timer? _timer;

    public DbMetricsUpdater(string dbName, DbOptions dbOptions, RocksDb db, ColumnFamilyHandle? cf, IDbConfig dbConfig, ILogger logger, Func<string> statsFetcher)
    {
        _dbName = dbName;
        _dbOptions = dbOptions;
        _db = db;
        _dbConfig = dbConfig;
        _columnFamilyHandle = cf;
        _logger = logger;
        _statsFetcher = statsFetcher;
    }

    public void StartUpdating()
    {
        var offsetInSec = _dbConfig.StatsDumpPeriodSec * 1.1;

        _timer = new Timer(UpdateMetrics, null, TimeSpan.FromSeconds(offsetInSec), TimeSpan.FromSeconds(offsetInSec));
    }

    private Prometheus.Gauge DBMetricsUpdater =
        Prometheus.Metrics.CreateGauge("db_metrics_updater", "The thing", "dbname", "themetric");
    private Prometheus.Gauge DBMetricsUpdaterByLevel =
        Prometheus.Metrics.CreateGauge("db_metrics_updater_bylevel", "The thing", "dbname", "level", "themetric");
    private Prometheus.Gauge DBMetricsUpdaterStats =
        Prometheus.Metrics.CreateGauge("db_metrics_updater_stats", "The thing", "dbname", "metric", "valuetype");
    private Prometheus.Gauge DBMetricsUpdaterSizes =
        Prometheus.Metrics.CreateGauge("db_metrics_updater_sizes", "The thing", "dbname", "sizeof");

    private readonly Func<string> _statsFetcher;

    private void UpdateMetrics(object? state)
    {
        try
        {
            // It seems that currently there is no other option with .NET api to extract the compaction statistics than through the dumped string
            var compactionStatsString = "";
            if (_columnFamilyHandle != null)
            {
                compactionStatsString = _db.GetProperty("rocksdb.stats", _columnFamilyHandle);
            }
            else
            {
                compactionStatsString = _db.GetProperty("rocksdb.stats");
            }
            ProcessCompactionStats(compactionStatsString);

            if (_dbConfig.EnableDbStatistics)
            {
                var dbStatsString = _statsFetcher();
                foreach ((string Name, List<(string, double)> Value) value in ExtractStatsFromStatisticString(dbStatsString))
                {
                    foreach ((string, double) valueTuple in value.Value)
                    {
                        DBMetricsUpdaterStats.WithLabels(_dbName, value.Name, valueTuple.Item1).Set(valueTuple.Item2);
                    }
                }
                // Currently we don't extract any DB statistics but we can do it here
            }

            DBMetricsUpdaterSizes.WithLabels(_dbName, "files").Set(GetSize());
            DBMetricsUpdaterSizes.WithLabels(_dbName, "caches").Set(GetCacheSize());
            DBMetricsUpdaterSizes.WithLabels(_dbName, "table-readers").Set(GetIndexSize());
            DBMetricsUpdaterSizes.WithLabels(_dbName, "memtable").Set(GetMemtableSize());
        }
        catch (Exception exc)
        {
            _logger.Error($"Error when updating metrics for {_dbName} database.", exc);
            // Maybe we would like to stop the _timer here to avoid logging the same error all over again?
        }
    }

    public long GetPropertyLong(string propertyName)
    {
        try
        {
            if (_columnFamilyHandle == null)
            {
                return long.TryParse(_db.GetProperty(propertyName), out long size) ? size : 0;
            }
            else
            {
                return long.TryParse(_db.GetProperty(propertyName, _columnFamilyHandle), out long size) ? size : 0;
            }
        }
        catch (RocksDbSharpException e)
        {
            if (_logger.IsWarn)
                _logger.Warn($"Failed to update DB size metrics {e.Message}");
        }

        return 0;
    }

    public long GetSize()
    {
        return GetPropertyLong("rocksdb.total-sst-files-size");
    }

    public long GetCacheSize()
    {
        return GetPropertyLong("rocksdb.block-cache-usage");
    }

    public long GetIndexSize()
    {
        return GetPropertyLong("rocksdb.estimate-table-readers-mem");
    }

    public long GetMemtableSize()
    {
        return GetPropertyLong("rocksdb.cur-size-all-mem-tables");
    }

    public void ProcessCompactionStats(string compactionStatsString)
    {
        if (!string.IsNullOrEmpty(compactionStatsString))
        {
            var stats = ExtractStatsPerLevel(compactionStatsString);
            UpdateMetricsFromList(stats);

            stats = ExctractIntervalCompaction(compactionStatsString);

            UpdateMetricsFromList(stats);
        }
        else
        {
            _logger.Warn($"No RocksDB compaction stats available for {_dbName} databse.");
        }
    }

    private void UpdateMetricsFromList(List<(string Name, double Value)> levelStats)
    {
        if (levelStats is not null)
        {
            foreach (var stat in levelStats)
            {
                DBMetricsUpdater.WithLabels(_dbName, stat.Name).Set(stat.Value);
                Metrics.DbStats[$"{_dbName}Db{stat.Name}"] = (long) stat.Value;
            }
        }
    }

    public static List<(string Name, List<(string, double)> Value)> ExtractStatsFromStatisticString(string compactionStatsDump)
    {
        var matches = ExtractStatsRegex2().Matches(compactionStatsDump);
        return matches.Select((match) =>
        {
            var statName = match.Groups[1].Value;
            var match2 = ExtractStatsRegex3().Matches(match.Groups[2].Value);

            var value = match2.Select((it) => (it.Groups[1].Value, double.Parse(it.Groups[2].Value))).ToList();

            return (statName, value);
        }).ToList();
    }

    /// <summary>
    /// Example line:
     // Level    Files   Size     Score Read(GB)  Rn(GB) Rnp1(GB) Write(GB) Wnew(GB) Moved(GB) W-Amp Rd(MB/s) Wr(MB/s) Comp(sec) CompMergeCPU(sec) Comp(cnt) Avg(sec) KeyIn KeyDrop
    ///   L0      2/0    1.77 MB   0.5      0.0     0.0      0.0       0.4      0.4       0.0   1.0      0.0     44.6      9.83              0.00       386    0.025       0      0
    /// </summary>
    private List<(string Name, double Value)> ExtractStatsPerLevel(string compactionStatsDump)
    {
        var stats = new List<(string Name, double Value)>(5);

        if (!string.IsNullOrEmpty(compactionStatsDump))
        {
            var rgx = ExtractStatsRegex();
            var matches = rgx.Matches(compactionStatsDump);

            foreach (Match m in matches)
            {
                string[] tabSplitted = m.Groups[0].Value.Split(new char[] {' '}, StringSplitOptions.RemoveEmptyEntries);
                var level = double.Parse(m.Groups[1].Value);
                stats.Add(($"Level{level}Files", double.Parse(m.Groups[2].Value)));
                stats.Add(($"Level{level}FilesCompacted", double.Parse(m.Groups[3].Value)));
                stats.Add(($"Level{level}Score", double.Parse(tabSplitted[4])));
                stats.Add(($"Level{level}Read", double.Parse(tabSplitted[5])));
                stats.Add(($"Level{level}Rn", double.Parse(tabSplitted[6])));
                stats.Add(($"Level{level}Rnp1", double.Parse(tabSplitted[7])));
                stats.Add(($"Level{level}Write", double.Parse(tabSplitted[8])));
                stats.Add(($"Level{level}Wnew", double.Parse(tabSplitted[9])));
                stats.Add(($"Level{level}Moved", double.Parse(tabSplitted[10])));
                stats.Add(($"Level{level}Wamp", double.Parse(tabSplitted[11])));
                stats.Add(($"Level{level}Rd", double.Parse(tabSplitted[12])));
                stats.Add(($"Level{level}Wr", double.Parse(tabSplitted[13])));
                stats.Add(($"Level{level}CompSec", double.Parse(tabSplitted[14])));
                stats.Add(($"Level{level}CompMergeCPU", double.Parse(tabSplitted[15])));
                stats.Add(($"Level{level}CompCnt", double.Parse(tabSplitted[16])));
                stats.Add(($"Level{level}Avg", double.Parse(tabSplitted[17])));

                var levelStr = m.Groups[1].Value;
                DBMetricsUpdaterByLevel.WithLabels(_dbName, levelStr, $"Files").Set(double.Parse(m.Groups[2].Value));
                DBMetricsUpdaterByLevel.WithLabels(_dbName, levelStr, $"FilesCompacted").Set(double.Parse(m.Groups[2].Value));
                DBMetricsUpdaterByLevel.WithLabels(_dbName, levelStr, $"Score").Set(double.Parse(tabSplitted[4]));
                DBMetricsUpdaterByLevel.WithLabels(_dbName, levelStr, $"Read").Set(double.Parse(tabSplitted[5]));
                DBMetricsUpdaterByLevel.WithLabels(_dbName, levelStr, $"Rn").Set(double.Parse(tabSplitted[6]));
                DBMetricsUpdaterByLevel.WithLabels(_dbName, levelStr, $"Rnp1").Set(double.Parse(tabSplitted[7]));
                DBMetricsUpdaterByLevel.WithLabels(_dbName, levelStr, $"Write").Set(double.Parse(tabSplitted[8]));
                DBMetricsUpdaterByLevel.WithLabels(_dbName, levelStr, $"Wnew").Set(double.Parse(tabSplitted[9]));
                DBMetricsUpdaterByLevel.WithLabels(_dbName, levelStr, $"Moved").Set(double.Parse(tabSplitted[10]));
                DBMetricsUpdaterByLevel.WithLabels(_dbName, levelStr, $"Wamp").Set(double.Parse(tabSplitted[11]));
                DBMetricsUpdaterByLevel.WithLabels(_dbName, levelStr, $"Rd").Set(double.Parse(tabSplitted[12]));
                DBMetricsUpdaterByLevel.WithLabels(_dbName, levelStr, $"Wr").Set(double.Parse(tabSplitted[13]));
                DBMetricsUpdaterByLevel.WithLabels(_dbName, levelStr, $"CompSec").Set(double.Parse(tabSplitted[14]));
                DBMetricsUpdaterByLevel.WithLabels(_dbName, levelStr, $"CompMergeCPU").Set(double.Parse(tabSplitted[15]));
                DBMetricsUpdaterByLevel.WithLabels(_dbName, levelStr, $"CompCnt").Set(double.Parse(tabSplitted[16]));
                DBMetricsUpdaterByLevel.WithLabels(_dbName, levelStr, $"Avg").Set(double.Parse(tabSplitted[17]));
            }
        }

        return stats;
    }

    /// <summary>
    /// Example line:
    /// Interval compaction: 0.00 GB write, 0.00 MB/s write, 0.00 GB read, 0.00 MB/s read, 0.0 seconds
    /// </summary>
    private List<(string Name, double Value)> ExctractIntervalCompaction(string compactionStatsDump)
    {
        var stats = new List<(string Name, double Value)>(5);

        if (!string.IsNullOrEmpty(compactionStatsDump))
        {
            var rgx = ExtractIntervalRegex();
            var match = rgx.Match(compactionStatsDump);

            if (match is not null && match.Success)
            {
                stats.Add(("IntervalCompactionGBWrite", double.Parse(match.Groups[1].Value)));
                stats.Add(("IntervalCompactionMBPerSecWrite", double.Parse(match.Groups[2].Value)));
                stats.Add(("IntervalCompactionGBRead", double.Parse(match.Groups[3].Value)));
                stats.Add(("IntervalCompactionMBPerSecRead", double.Parse(match.Groups[4].Value)));
                stats.Add(("IntervalCompactionSeconds", double.Parse(match.Groups[5].Value)));
            }
            else
            {
                _logger.Warn($"Cannot find 'Interval compaction' stats for {_dbName} database in the compation stats dump:{Environment.NewLine}{compactionStatsDump}");
            }
        }

        return stats;
    }

    [GeneratedRegex("^\\s+L(\\d+)\\s+(\\d+)\\/(\\d+).*$", RegexOptions.Multiline)]
    private static partial Regex ExtractStatsRegex();

    [GeneratedRegex("^(\\S+)(.*)$", RegexOptions.Multiline)]
    private static partial Regex ExtractStatsRegex2();

    [GeneratedRegex("(\\S+) \\: (\\S+)", RegexOptions.Multiline)]
    private static partial Regex ExtractStatsRegex3();

    [GeneratedRegex("^Interval compaction: (\\d+)\\.\\d+.*GB write.*\\s+(\\d+)\\.\\d+.*MB\\/s write.*\\s+(\\d+)\\.\\d+.*GB read.*\\s+(\\d+)\\.\\d+.*MB\\/s read.*\\s+(\\d+)\\.\\d+.*seconds.*$", RegexOptions.Multiline)]
    private static partial Regex ExtractIntervalRegex();

    [GeneratedRegex("^Stall.*$", RegexOptions.Multiline)]
    private static partial Regex ExtractStallRegex();

    public void Dispose()
    {
        _timer?.Dispose();
    }
}
