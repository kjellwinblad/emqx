%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(emqx_bridge_action_mqtt_publisher_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, ref/2]).

-export([roots/0, fields/1, desc/1, namespace/0]).

%%======================================================================================
%% Hocon Schema Definitions
namespace() -> "bridge_mqtt_publisher".

roots() -> [].

fields(action) ->
    {mqtt_publisher,
        mk(
            hoconsc:map(name, ref(?MODULE, "mqtt_publisher_action")),
            #{
                desc => <<"MQTT Publisher Action Config">>,
                required => false
            }
        )};
fields("mqtt_publisher_action") ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable_bridge"), default => true})},
        {connector,
            mk(binary(), #{
                desc => ?DESC(emqx_connector_schema, "connector_field"), required => true
            })},
        {description, emqx_schema:description_schema()},
        %% Note: there's an implicit convention in `emqx_bridge' that,
        %% for egress bridges with this config, the published messages
        %% will be forwarded to such bridges.
        {local_topic,
            mk(
                binary(),
                #{
                    required => false,
                    desc => ?DESC("config_local_topic"),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        %% Since e5.3.2, we split the http bridge to two parts: a) connector. b) actions.
        %% some fields are moved to connector, some fields are moved to actions and composed into the
        %% `parameters` field.
        {parameters,
            hoconsc:mk(hoconsc:ref("parameters_opts"), #{
                required => true,
                desc => ?DESC("config_parameters_opts")
            })}
    ] ++ resource_opts();
fields("parameters_opts") ->
    [
        {path,
            mk(
                binary(),
                #{
                    desc => ?DESC("config_path"),
                    required => false
                }
            )}
    ];
fields("resource_opts") ->
    UnsupportedOpts = [enable_batch, batch_size, batch_time],
    lists:filter(
        fun({K, _V}) -> not lists:member(K, UnsupportedOpts) end,
        emqx_resource_schema:fields("creation_opts")
    );
fields(_What) ->
    x:show(missing_field, _What),
    erlang:halt().
%% v2: api schema
%% The parameter equls to
%%   `get_bridge_v2`, `post_bridge_v2`, `put_bridge_v2` from emqx_bridge_v2_schema:api_schema/1
%%   `get_connector`, `post_connector`, `put_connector` from emqx_connector_schema:api_schema/1
%%--------------------------------------------------------------------
%% v1/v2

desc("config") ->
    ?DESC("desc_config");
desc("resource_opts") ->
    ?DESC(emqx_resource_schema, "creation_opts");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for WebHook using `", string:to_upper(Method), "` method."];
desc("config_connector") ->
    ?DESC("desc_config");
desc("http_action") ->
    ?DESC("desc_config");
desc("parameters_opts") ->
    ?DESC("config_parameters_opts");
desc(_) ->
    undefined.

resource_opts() ->
    [
        {resource_opts,
            mk(
                ref(?MODULE, "resource_opts"),
                #{
                    required => false,
                    default => #{},
                    desc => ?DESC(emqx_resource_schema, <<"resource_opts">>)
                }
            )}
    ].
