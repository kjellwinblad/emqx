%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% access module for transaction logs
%% implemented as a ram_copies mnesia table
-module(emqx_rlog_tab).

%% Mnesia bootstrap
-export([mnesia/1]).

-export([write/3, first_d/1, last_d/1, next_d/2]).

-export_type([ change_type/0
             , op/0
             ]).

-type key() :: emqx_rlog:key().
-type table_name():: atom().
-type change_type() :: write | delete | delete_object.
-type op() :: {{table_name(), term()}, term(), change_type()}.
-type shard() :: emqx_tx:shard().

-include("emqx_rlog.hrl").

-record(rlog,
        { key :: key() %% key should be monotoic and globally unique
        , ops :: [op()]
        }).

%% @doc Mnesia bootstrap.
mnesia(boot) ->
    Opts = [ {type, ordered_set}
           , {ram_copies, [node()]}
           , {record_name, rlog}
           , {attributes, record_info(fields, rlog)}
           ],
    ok = ekka_mnesia:create_table(?SHARD_ROUTING, Opts).

%% @doc Write a transaction log.
-spec write(shard(), key(), [op()]) -> ok.
write(Shard, Key, Ops) ->
    Log = #rlog{ key = Key
               , ops = Ops
               },
    mnesia:write(Shard, Log, write).

%% @doc Search for the first record in the table.
-spec first_d(shard()) -> [key()].
first_d(Shard) ->
    case mnesia:dirty_first(Shard) of
        '$end_of_table' -> [];
        Key -> [Key]
    end.

%% @doc Search for the last key in the table.
-spec last_d(shard()) -> [key()].
last_d(Shard) ->
    case mnesia:dirty_last(Shard) of
        '$end_of_table' -> [];
        Key -> [Key]
    end.

%% @doc Search for the next key ordered immediately behind the given one.
-spec next_d(shard(), key()) -> [key()].
next_d(Shard, Key) ->
    case mnesia:dirty_next(Shard, Key) of
        '$end_of_table' -> [];
        Key -> [Key]
    end.
