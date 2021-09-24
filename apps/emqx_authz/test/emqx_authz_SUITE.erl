%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(CONF_DEFAULT, <<"authorization: {sources: []}">>).

all() ->
    emqx_ct:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    meck:new(emqx_schema, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_schema, fields, fun("authorization") ->
                                             meck:passthrough(["authorization"]) ++
                                             emqx_authz_schema:fields("authorization");
                                        (F) -> meck:passthrough([F])
                                     end),

    meck:new(emqx_resource, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_resource, create, fun(_, _, _) -> {ok, meck_data} end),
    meck:expect(emqx_resource, update, fun(_, _, _, _) -> {ok, meck_data} end),
    meck:expect(emqx_resource, remove, fun(_) -> ok end ),

    ok = emqx_config:init_load(emqx_authz_schema, ?CONF_DEFAULT),
    ok = emqx_ct_helpers:start_apps([emqx_authz]),
    {ok, _} = emqx:update_config([authorization, cache, enable], false),
    {ok, _} = emqx:update_config([authorization, no_match], deny),
    Config.

end_per_suite(_Config) ->
    {ok, _} = emqx_authz:update(?CMD_REPLCAE, []),
    emqx_ct_helpers:stop_apps([emqx_authz, emqx_resource]),
    meck:unload(emqx_resource),
    meck:unload(emqx_schema),
    ok.

init_per_testcase(_, Config) ->
    {ok, _} = emqx_authz:update(?CMD_REPLCAE, []),
    Config.

-define(SOURCE1, #{<<"type">> => <<"http">>,
                   <<"enable">> => true,
                   <<"url">> => <<"https://fake.com:443/">>,
                   <<"headers">> => #{},
                   <<"method">> => <<"get">>,
                   <<"request_timeout">> => 5000
                  }).
-define(SOURCE2, #{<<"type">> => <<"mongodb">>,
                   <<"enable">> => true,
                   <<"mongo_type">> => <<"single">>,
                   <<"server">> => <<"127.0.0.1:27017">>,
                   <<"pool_size">> => 1,
                   <<"database">> => <<"mqtt">>,
                   <<"ssl">> => #{<<"enable">> => false},
                   <<"collection">> => <<"fake">>,
                   <<"selector">> => #{<<"a">> => <<"b">>}
                  }).
-define(SOURCE3, #{<<"type">> => <<"mysql">>,
                   <<"enable">> => true,
                   <<"server">> => <<"127.0.0.1:27017">>,
                   <<"pool_size">> => 1,
                   <<"database">> => <<"mqtt">>,
                   <<"username">> => <<"xx">>,
                   <<"password">> => <<"ee">>,
                   <<"auto_reconnect">> => true,
                   <<"ssl">> => #{<<"enable">> => false},
                   <<"query">> => <<"abcb">>
                  }).
-define(SOURCE4, #{<<"type">> => <<"postgresql">>,
                   <<"enable">> => true,
                   <<"server">> => <<"127.0.0.1:27017">>,
                   <<"pool_size">> => 1,
                   <<"database">> => <<"mqtt">>,
                   <<"username">> => <<"xx">>,
                   <<"password">> => <<"ee">>,
                   <<"auto_reconnect">> => true,
                   <<"ssl">> => #{<<"enable">> => false},
                   <<"query">> => <<"abcb">>
                  }).
-define(SOURCE5, #{<<"type">> => <<"redis">>,
                   <<"enable">> => true,
                   <<"server">> => <<"127.0.0.1:27017">>,
                   <<"pool_size">> => 1,
                   <<"database">> => 0,
                   <<"password">> => <<"ee">>,
                   <<"auto_reconnect">> => true,
                   <<"ssl">> => #{<<"enable">> => false},
                   <<"cmd">> => <<"HGETALL mqtt_authz:%u">>
                  }).
-define(SOURCE6, #{<<"type">> => <<"file">>,
                   <<"enable">> => true,
                   <<"path">> => emqx_ct_helpers:deps_path(emqx_authz, "etc/acl.conf")
                  }).


%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_update_source(_) ->
    {ok, _} = emqx_authz:update(?CMD_REPLCAE, [?SOURCE3]),
    {ok, _} = emqx_authz:update(?CMD_PREPEND, [?SOURCE2]),
    {ok, _} = emqx_authz:update(?CMD_PREPEND, [?SOURCE1]),
    {ok, _} = emqx_authz:update(?CMD_APPEND, [?SOURCE4]),
    {ok, _} = emqx_authz:update(?CMD_APPEND, [?SOURCE5]),
    {ok, _} = emqx_authz:update(?CMD_APPEND, [?SOURCE6]),

    ?assertMatch([ #{type := http,  enable := true}
                 , #{type := mongodb, enable := true}
                 , #{type := mysql, enable := true}
                 , #{type := postgresql, enable := true}
                 , #{type := redis, enable := true}
                 , #{type := file,  enable := true}
                 ], emqx:get_config([authorization, sources], [])),

    {ok, _} = emqx_authz:update({?CMD_REPLCAE, http},  ?SOURCE1#{<<"enable">> := false}),
    {ok, _} = emqx_authz:update({?CMD_REPLCAE, mongodb}, ?SOURCE2#{<<"enable">> := false}),
    {ok, _} = emqx_authz:update({?CMD_REPLCAE, mysql}, ?SOURCE3#{<<"enable">> := false}),
    {ok, _} = emqx_authz:update({?CMD_REPLCAE, postgresql}, ?SOURCE4#{<<"enable">> := false}),
    {ok, _} = emqx_authz:update({?CMD_REPLCAE, redis}, ?SOURCE5#{<<"enable">> := false}),
    {ok, _} = emqx_authz:update({?CMD_REPLCAE, file},  ?SOURCE6#{<<"enable">> := false}),

    ?assertMatch([ #{type := http,  enable := false}
                 , #{type := mongodb, enable := false}
                 , #{type := mysql, enable := false}
                 , #{type := postgresql, enable := false}
                 , #{type := redis, enable := false}
                 , #{type := file,  enable := false}
                 ], emqx:get_config([authorization, sources], [])),

    {ok, _} = emqx_authz:update(?CMD_REPLCAE, []).

t_move_source(_) ->
    {ok, _} = emqx_authz:update(?CMD_REPLCAE, [?SOURCE1, ?SOURCE2, ?SOURCE3, ?SOURCE4, ?SOURCE5, ?SOURCE6]),
    ?assertMatch([ #{type := http}
                 , #{type := mongodb}
                 , #{type := mysql}
                 , #{type := postgresql}
                 , #{type := redis}
                 , #{type := file}
                 ], emqx_authz:lookup()),

    {ok, _} = emqx_authz:move(postgresql, <<"top">>),
    ?assertMatch([ #{type := postgresql}
                 , #{type := http}
                 , #{type := mongodb}
                 , #{type := mysql}
                 , #{type := redis}
                 , #{type := file}
                 ], emqx_authz:lookup()),

    {ok, _} = emqx_authz:move(http, <<"bottom">>),
    ?assertMatch([ #{type := postgresql}
                 , #{type := mongodb}
                 , #{type := mysql}
                 , #{type := redis}
                 , #{type := file}
                 , #{type := http}
                 ], emqx_authz:lookup()),

    {ok, _} = emqx_authz:move(mysql, #{<<"before">> => postgresql}),
    ?assertMatch([ #{type := mysql}
                 , #{type := postgresql}
                 , #{type := mongodb}
                 , #{type := redis}
                 , #{type := file}
                 , #{type := http}
                 ], emqx_authz:lookup()),

    {ok, _} = emqx_authz:move(mongodb, #{<<"after">> => http}),
    ?assertMatch([ #{type := mysql}
                 , #{type := postgresql}
                 , #{type := redis}
                 , #{type := file}
                 , #{type := http}
                 , #{type := mongodb}
                 ], emqx_authz:lookup()),

    ok.
