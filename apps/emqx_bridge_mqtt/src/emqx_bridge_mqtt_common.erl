%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_bridge_mqtt_common).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").

-export([
    mk_client_opts/3
]).

mk_client_opts(
    ResourceId,
    ClientScope,
    Config = #{
        server := Server,
        keepalive := KeepAlive,
        ssl := #{enable := EnableSsl} = Ssl
    }
) ->
    HostPort = emqx_bridge_mqtt_connector_schema:parse_server(Server),
    Options = maps:with(
        [
            proto_ver,
            username,
            password,
            clean_start,
            retry_interval,
            max_inflight,
            % Opening a connection in bridge mode will form a non-standard mqtt connection message.
            % A load balancing server (such as haproxy) is often set up before the emqx broker server.
            % When the load balancing server enables mqtt connection packet inspection,
            % non-standard mqtt connection packets might be filtered out by LB.
            bridge_mode
        ],
        Config
    ),
    mk_client_opt_password(Options#{
        hosts => [HostPort],
        clientid => clientid(ResourceId, ClientScope, Config),
        connect_timeout => 30,
        keepalive => ms_to_s(KeepAlive),
        force_ping => true,
        ssl => EnableSsl,
        ssl_opts => maps:to_list(maps:remove(enable, Ssl))
    }).

mk_client_opt_password(Options = #{password := Secret}) ->
    %% TODO: Teach `emqtt` to accept 0-arity closures as passwords.
    Options#{password := emqx_secret:unwrap(Secret)};
mk_client_opt_password(Options) ->
    Options.

ms_to_s(Ms) ->
    erlang:ceil(Ms / 1000).

clientid(Id, ClientScope, _Conf = #{clientid_prefix := Prefix}) when is_binary(Prefix) ->
    iolist_to_binary([Prefix, ":", Id, ":", ClientScope, ":", atom_to_list(node())]);
clientid(Id, ClientScope, _Conf) ->
    iolist_to_binary([Id, ":", ClientScope, ":", atom_to_list(node())]).
