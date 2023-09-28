%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_bridge_v2_CRUD_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    ok = emqx_common_test_helpers:start_apps(apps_to_start_and_stop()),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps(apps_to_start_and_stop()).

apps_to_start_and_stop() ->
    [
        emqx,
        emqx_conf,
        emqx_connector,
        emqx_bridge,
        emqx_rule_engine
    ].

t_create_remove_list(_) ->
    [] = emqx_bridge_v2:list(),
    ConnectorConfig = connector_config(),
    {ok, _} = emqx_connector:create(kafka, test_connector, ConnectorConfig),
    Config = bridge_v2_config(),
    {ok, _Config} = emqx_bridge_v2:create(kafka, test_bridge_v2, Config),
    [BridgeV2Info] = emqx_bridge_v2:list(),
    #{
        name := <<"test_bridge_v2">>,
        type := <<"kafka">>,
        raw_config := _RawConfig
    } = BridgeV2Info,
    {ok, _Config2} = emqx_bridge_v2:create(kafka, test_bridge_v2_2, Config),
    2 = length(emqx_bridge_v2:list()),
    {ok, _} = emqx_bridge_v2:remove(kafka, test_bridge_v2),
    1 = length(emqx_bridge_v2:list()),
    {ok, _} = emqx_bridge_v2:remove(kafka, test_bridge_v2_2),
    [] = emqx_bridge_v2:list(),
    ok.

%% Test sending a message to a bridge V2
% t_send_message(_) ->
%     BridgeV2Config = bridge_v2_config(),
%     ConnectorConfig = connector_config(),
%     {ok, _} = emqx_connector:create(kafka, test_connector, ConnectorConfig),
%     {ok, _} = emqx_bridge_v2:create(kafka, test_bridge_v2, BridgeV2Config),
%     BridgeV2Id = emqx_bridge_v2:id(kafka, test_bridge_v2),
%     {ok, _} = emqx_bridge_v2:send_message(kafka, test_bridge_v2, {BridgeV2Id, <<"test">>}, #{}),
%     {ok, _} = emqx_bridge_v2:remove(kafka, test_bridge_v2),
%     ok.

bridge_v2_config() ->
    #{
        <<"connector">> => <<"test_connector">>,
        <<"enable">> => true,
        <<"kafka">> => #{
            <<"buffer">> => #{
                <<"memory_overload_protection">> => false,
                <<"mode">> => <<"memory">>,
                <<"per_partition_limit">> => <<"2GB">>,
                <<"segment_bytes">> => <<"100MB">>
            },
            <<"compression">> => <<"no_compression">>,
            <<"kafka_header_value_encode_mode">> => <<"none">>,
            <<"max_batch_bytes">> => <<"896KB">>,
            <<"max_inflight">> => 10,
            <<"message">> => #{
                <<"key">> => <<"${.clientid}">>,
                <<"timestamp">> => <<"${.timestamp}">>,
                <<"value">> => <<"${.}">>
            },
            <<"partition_count_refresh_interval">> => <<"60s">>,
            <<"partition_strategy">> => <<"random">>,
            <<"query_mode">> => <<"async">>,
            <<"required_acks">> => <<"all_isr">>,
            <<"sync_query_timeout">> => <<"5s">>,
            <<"topic">> => <<"testtopic-in">>
        },
        <<"local_topic">> => <<"kafka_t/#">>,
        <<"resource_opts">> => #{
            <<"health_check_interval">> => <<"15s">>
        }
    }.

connector_config() ->
    #{
        <<"authentication">> => <<"none">>,
        <<"bootstrap_hosts">> => <<"127.0.0.1:9092">>,
        <<"connect_timeout">> => <<"5s">>,
        <<"enable">> => true,
        <<"metadata_request_timeout">> => <<"5s">>,
        <<"min_metadata_refresh_interval">> => <<"3s">>,
        <<"socket_opts">> =>
            #{
                <<"recbuf">> => <<"1024KB">>,
                <<"sndbuf">> => <<"1024KB">>,
                <<"tcp_keepalive">> => <<"none">>
            },
        <<"ssl">> =>
            #{
                <<"ciphers">> => [],
                <<"depth">> => 10,
                <<"enable">> => false,
                <<"hibernate_after">> => <<"5s">>,
                <<"log_level">> => <<"notice">>,
                <<"reuse_sessions">> => true,
                <<"secure_renegotiate">> => true,
                <<"verify">> => <<"verify_peer">>,
                <<"versions">> => [<<"tlsv1.3">>, <<"tlsv1.2">>]
            }
    }.
