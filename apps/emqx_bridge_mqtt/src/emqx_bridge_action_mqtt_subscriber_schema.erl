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
-module(emqx_bridge_action_mqtt_subscriber_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, ref/2]).

-export([roots/0, fields/1, desc/1, namespace/0]).

-export([
    bridge_v2_examples/1,
    conn_bridge_examples/1
]).

%%======================================================================================
%% Hocon Schema Definitions
namespace() -> "bridge_mqtt_subscriber".

roots() -> [].

fields(action) ->
    {mqtt_subscriber,
        mk(
            hoconsc:map(name, ref(?MODULE, "mqtt_subscriber_action")),
            #{
                desc => <<"MQTT Publisher Action Config">>,
                required => false
            }
        )};
fields("mqtt_subscriber_action") ->
    emqx_bridge_v2_schema:make_consumer_action_schema(
        hoconsc:mk(
            hoconsc:ref(?MODULE, action_parameters),
            #{
                required => true,
                desc => ?DESC("action_parameters")
            }
        )
    );
fields(action_parameters) ->
    emqx_bridge_mqtt_connector_schema:fields("ingress");
fields("resource_opts") ->
    UnsupportedOpts = [enable_batch, batch_size, batch_time],
    lists:filter(
        fun({K, _V}) -> not lists:member(K, UnsupportedOpts) end,
        emqx_resource_schema:fields("creation_opts")
    );
fields("get_bridge_v2") ->
    fields("mqtt_subscriber_action");
fields("post_bridge_v2") ->
    fields("mqtt_subscriber_action");
fields("put_bridge_v2") ->
    fields("mqtt_subscriber_action");
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

bridge_v2_examples(_Method) ->
    [
        #{}
    ].

conn_bridge_examples(_Method) ->
    [
        #{}
    ].
