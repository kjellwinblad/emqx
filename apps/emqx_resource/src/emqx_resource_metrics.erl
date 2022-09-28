-module(emqx_resource_metrics).

-export([
    batching_inc/1,
    batching_inc/2,
    dropped_inc/1,
    dropped_inc/2,
    dropped_other_inc/1,
    dropped_other_inc/2,
    dropped_queue_full_inc/1,
    dropped_queue_full_inc/2,
    dropped_queue_not_enabled_inc/1,
    dropped_queue_not_enabled_inc/2,
    dropped_resource_not_found_inc/1,
    dropped_resource_not_found_inc/2,
    dropped_resource_stopped_inc/1,
    dropped_resource_stopped_inc/2,
    failed_inc/1,
    failed_inc/2,
    inflight_inc/1,
    inflight_inc/2,
    matched_inc/1,
    matched_inc/2,
    queuing_inc/1,
    queuing_inc/2,
    retried_inc/1,
    retried_inc/2,
    retried_failed_inc/1,
    retried_failed_inc/2,
    retried_success_inc/1,
    retried_success_inc/2,
    success_inc/1,
    success_inc/2
]).

-define(RES_METRICS, resource_metrics).

batching_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'batching').

batching_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'batching', Val).

dropped_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped').

dropped_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped', Val).

dropped_other_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.other').

dropped_other_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.other', Val).

dropped_queue_full_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.queue_full').

dropped_queue_full_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.queue_full', Val).

dropped_queue_not_enabled_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.queue_not_enabled').

dropped_queue_not_enabled_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.queue_not_enabled', Val).

dropped_resource_not_found_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.resource_not_found').

dropped_resource_not_found_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.resource_not_found', Val).

dropped_resource_stopped_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.resource_stopped').

dropped_resource_stopped_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.resource_stopped', Val).

matched_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'matched').

matched_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'matched', Val).

queuing_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'queuing').

queuing_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'queuing', Val).

retried_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'retried').

retried_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'retried', Val).

failed_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'failed').

failed_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'failed', Val).

inflight_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'inflight').

inflight_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'inflight', Val).

retried_failed_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'retried.failed').

retried_failed_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'retried.failed', Val).

retried_success_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'retried.success').

retried_success_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'retried.success', Val).

success_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'success').

success_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'success', Val).
