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
-module(emqx_connector_schema_lib).

-include("emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([
    relational_db_fields/0,
    ssl_fields/0,
    prepare_statement_fields/0,
    connector_relational_db_fields/0,
    bridge_db_fields/0
]).

-export([
    pool_size/1,
    database/1,
    username/1,
    password/1,
    password_required/1,
    auto_reconnect/1
]).

-type database() :: binary().
-type pool_size() :: pos_integer().
-type username() :: binary().
-type password() :: binary().

-reflect_type([
    database/0,
    pool_size/0,
    username/0,
    password/0
]).

-export([roots/0, fields/1]).

roots() -> [].

fields(_) -> [].

ssl_fields() ->
    [
        {ssl, #{
            type => hoconsc:ref(emqx_schema, "ssl_client_opts"),
            default => #{<<"enable">> => false},
            desc => ?DESC("ssl")
        }}
    ].

relational_db_fields() ->
    connector_relational_db_fields() ++ bridge_db_fields().

connector_relational_db_fields() ->
    [
        %% TODO: The `pool_size` for drivers will be deprecated. Ues `worker_pool_size` for emqx_resource
        %% See emqx_resource.hrl
        {pool_size, fun pool_size/1},
        {username, fun username/1},
        {password, fun password/1},
        {auto_reconnect, fun auto_reconnect/1}
    ].

bridge_db_fields() ->
    [
        {database, fun database/1}
    ].

prepare_statement_fields() ->
    [{prepare_statement, fun prepare_statement/1}].

prepare_statement(type) -> map();
prepare_statement(desc) -> ?DESC("prepare_statement");
prepare_statement(required) -> false;
prepare_statement(_) -> undefined.

database(type) -> binary();
database(desc) -> ?DESC("database_desc");
database(required) -> true;
database(validator) -> [?NOT_EMPTY("the value of the field 'database' cannot be empty")];
database(_) -> undefined.

pool_size(type) -> pos_integer();
pool_size(desc) -> ?DESC("pool_size");
pool_size(default) -> 8;
pool_size(validator) -> [?MIN(1)];
pool_size(_) -> undefined.

username(type) -> binary();
username(desc) -> ?DESC("username");
username(required) -> false;
username(_) -> undefined.

password(type) -> binary();
password(desc) -> ?DESC("password");
password(required) -> false;
password(format) -> <<"password">>;
password(sensitive) -> true;
password(converter) -> fun emqx_schema:password_converter/2;
password(_) -> undefined.

password_required(type) -> binary();
password_required(desc) -> ?DESC("password");
password_required(required) -> true;
password_required(format) -> <<"password">>;
password_required(sensitive) -> true;
password_required(converter) -> fun emqx_schema:password_converter/2;
password_required(_) -> undefined.

auto_reconnect(type) -> boolean();
auto_reconnect(desc) -> ?DESC("auto_reconnect");
auto_reconnect(default) -> true;
auto_reconnect(deprecated) -> {since, "v5.0.15"};
auto_reconnect(_) -> undefined.
