// SPDX-FileCopyrightText: 2023 Demerzel Solutions Limited
// SPDX-License-Identifier: LGPL-3.0-only

using System;
using System.Diagnostics;
using System.Threading;
using Prometheus;

namespace Nethermind.Core.Memory;

public class MemoryPressureHelper : IMemoryPressureHelper
{
    public static IMemoryPressureHelper Instance = new MemoryPressureHelper();

    private readonly TimeSpan _updateInterval = TimeSpan.FromSeconds(1);
    const double HighPressureThreshold = .90;       // Percent of GC memory pressure threshold we consider "high"
    const double MediumPressureThreshold = .70;     // Percent of GC memory pressure threshold we consider "medium"

    private long _lastMeasurementTime = 0;
    private IMemoryPressureHelper.MemoryPressure _lastPressure = IMemoryPressureHelper.MemoryPressure.Low;

    private static Counter MemoryPressureCount = Metrics.CreateCounter("memory_pressure_helper_count", "count", "pressure");
    private static Gauge MemoryPressureGauge = Metrics.CreateGauge("memory_pressure_gauge", "count", "reading");

    public IMemoryPressureHelper.MemoryPressure GetCurrentMemoryPressure()
    {
        UpdateMemoryPressureIfNeeded();

        return _lastPressure;
    }

    private void UpdateMemoryPressureIfNeeded()
    {
        // Debounce logic to prevent getting GC memory info all the time
        long thisLastMeasurementTime = _lastMeasurementTime;
        if (Stopwatch.GetElapsedTime(thisLastMeasurementTime) <= _updateInterval ||
            Interlocked.CompareExchange(ref _lastMeasurementTime, Stopwatch.GetTimestamp(), thisLastMeasurementTime) !=
            thisLastMeasurementTime) return;

        // Logic from https://github.com/dotnet/runtime/blob/a891aed8e03a225d8b31bbea0ae1a86972adc819/src/libraries/System.Private.CoreLib/src/System/Buffers/Utilities.cs#L40
        GCMemoryInfo memoryInfo = GC.GetGCMemoryInfo();
        if (memoryInfo.MemoryLoadBytes >= memoryInfo.HighMemoryLoadThresholdBytes * HighPressureThreshold)
        {
            _lastPressure = IMemoryPressureHelper.MemoryPressure.High;
        }
        else if (memoryInfo.MemoryLoadBytes >= memoryInfo.HighMemoryLoadThresholdBytes * MediumPressureThreshold)
        {
            _lastPressure = IMemoryPressureHelper.MemoryPressure.Medium;
        }
        else
        {
            _lastPressure = IMemoryPressureHelper.MemoryPressure.Low;
        }

        UpdateMetrics();
        MemoryPressureCount.WithLabels(_lastPressure.ToString()).Inc();
    }

    public static void UpdateMetrics()
    {
        GCMemoryInfo memoryInfo = GC.GetGCMemoryInfo();
        MemoryPressureGauge.WithLabels("load").Set(memoryInfo.MemoryLoadBytes);
        MemoryPressureGauge.WithLabels("high").Set(memoryInfo.HighMemoryLoadThresholdBytes);
        MemoryPressureGauge.WithLabels("fragmented").Set(memoryInfo.FragmentedBytes);
        MemoryPressureGauge.WithLabels("available").Set(memoryInfo.TotalAvailableMemoryBytes);
        MemoryPressureGauge.WithLabels("committed").Set(memoryInfo.TotalCommittedBytes);
        MemoryPressureGauge.WithLabels("heap").Set(memoryInfo.HeapSizeBytes);
    }
}

public interface IMemoryPressureHelper
{
    public enum MemoryPressure
    {
        Low,
        Medium,
        High
    }

    MemoryPressure GetCurrentMemoryPressure();
}
