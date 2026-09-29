param([string]$Load, [string]$Samples, [string]$DockerStats)
function Metric($item, $prefix) {
    $p = $item.PSObject.Properties | Where-Object { $_.Name.StartsWith($prefix) } | Select-Object -First 1
    if ($p) { return [double]$p.Value }
    return 0
}
$loadData = Get-Content -Raw -LiteralPath $Load | ConvertFrom-Json
$start = [datetimeoffset]$loadData.started_at
$samplesData = Get-Content -LiteralPath $Samples | ForEach-Object { $_ | ConvertFrom-Json }
$before = $samplesData | Where-Object { ([datetimeoffset]$_.at - $start).TotalSeconds -le 0 } | Select-Object -Last 1
$after = $samplesData | Where-Object { ([datetimeoffset]$_.at - $start).TotalSeconds -ge 10 } | Select-Object -First 1
$window = @($samplesData | Where-Object { ([datetimeoffset]$_.at - $start).TotalSeconds -ge 0 -and ([datetimeoffset]$_.at - $start).TotalSeconds -le 10 })
"actual_start=$($start.ToString('o')) start_sample=$($before.at) end_sample=$($after.at) window_samples=$($window.Count)"
foreach ($name in @('app1','app2')) {
    $a = $before.$name
    $b = $after.$name
    $dbCount = (Metric $b 'im_db_operation_seconds_count{result="ok"}') - (Metric $a 'im_db_operation_seconds_count{result="ok"}')
    $dbSeconds = (Metric $b 'im_db_operation_seconds_sum{result="ok"}') - (Metric $a 'im_db_operation_seconds_sum{result="ok"}')
    $peakRSS = ($window | ForEach-Object { $_.$name.process_resident_memory_bytes } | Measure-Object -Maximum).Maximum
    $peakG = ($window | ForEach-Object { $_.$name.go_goroutines } | Measure-Object -Maximum).Maximum
    [pscustomobject]@{
        app=$name
        publish_delta=(Metric $b 'im_kafka_publish_total{result="ok"}') - (Metric $a 'im_kafka_publish_total{result="ok"}')
        consume_delta=(Metric $b 'im_kafka_consume_total{result="ok"}') - (Metric $a 'im_kafka_consume_total{result="ok"}')
        db_query_delta=$dbCount
        db_avg_ms=if($dbCount -gt 0){[math]::Round(1000*$dbSeconds/$dbCount,3)}else{0}
        cpu_seconds=[math]::Round($b.process_cpu_seconds_total-$a.process_cpu_seconds_total,3)
        rss_peak_mib=[math]::Round($peakRSS/1MB,1)
        goroutine_peak=$peakG
    } | Format-List
}
"pg_commit_delta=$($after.pg_xact_commit-$before.pg_xact_commit) pg_rollback_delta=$($after.pg_xact_rollback-$before.pg_xact_rollback) kafka_end_delta=$($after.kafka_end_offset-$before.kafka_end_offset)"
"pg_connections_max=$(($window | Measure-Object pg_connections -Maximum).Maximum) pg_active_max=$(($window | Measure-Object pg_active -Maximum).Maximum) pg_waiting_max=$(($window | Measure-Object pg_waiting -Maximum).Maximum)"
"message_backlog_max=$(($window | Measure-Object message_backlog -Maximum).Maximum) status_backlog_max=$(($window | Measure-Object status_backlog -Maximum).Maximum) kafka_lag_app1_max=$(($window | Measure-Object kafka_lag_app1 -Maximum).Maximum) kafka_lag_app2_max=$(($window | Measure-Object kafka_lag_app2 -Maximum).Maximum)"
"time_seconds | message_backlog | status_backlog | kafka_end | lag_app1 | lag_app2"
$samplesData | ForEach-Object {
    $t = ([datetimeoffset]$_.at - $start).TotalSeconds
    if ($t -ge -1 -and $t -le 16) {
        '{0:n1} | {1} | {2} | {3} | {4} | {5}' -f $t,$_.message_backlog,$_.status_backlog,$_.kafka_end_offset,$_.kafka_lag_app1,$_.kafka_lag_app2
    }
}
if ($DockerStats -and (Test-Path -LiteralPath $DockerStats)) {
    $rows = Get-Content -LiteralPath $DockerStats | Select-Object -Skip 1 | ForEach-Object {
        $f = $_ -split '\|'
        if ($f.Count -ge 4) {
            [pscustomobject]@{t=([datetimeoffset]::Parse($f[0])-$start).TotalSeconds; name=$f[1]; cpu=[double]($f[2] -replace '%',''); mem=$f[3]}
        }
    }
    $rows | Where-Object { $_.t -ge 0 -and $_.t -le 10 -and $_.name -in @('deployments-app-1','deployments-app2-1','deployments-postgres-1','deployments-kafka-1') } |
        Group-Object name | ForEach-Object {
            [pscustomobject]@{name=$_.Name; samples=$_.Count; cpu_mean=[math]::Round(($_.Group|Measure-Object cpu -Average).Average,2); cpu_peak=[math]::Round(($_.Group|Measure-Object cpu -Maximum).Maximum,2); mem_last=$_.Group[-1].mem}
        } | Format-Table -AutoSize
}
