%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_dashboard_app).

-behaviour(application).

-export([ start/2
        , stop/1
        ]).

-include("emqx_dashboard.hrl").

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_dashboard_sup:start_link(),
    ok = mria_rlog:wait_for_shards([?DASHBOARD_SHARD], infinity),
    case emqx_dashboard:start_listeners() of
        ok ->
            emqx_dashboard_cli:load(),
            {ok, _Result} = emqx_dashboard_admin:add_default_user(),
            ok = emqx_dashboard_config:add_handler(),
            {ok, Sup};
        {error, Reason} -> {error, Reason}
    end.

stop(_State) ->
    ok = emqx_dashboard_config:remove_handler(),
    emqx_dashboard_cli:unload(),
    emqx_dashboard:stop_listeners().
