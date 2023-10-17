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
-module(emqx_connector).

-behaviour(emqx_config_handler).
-behaviour(emqx_config_backup).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    pre_config_update/3,
    post_config_update/5
]).

-export([
    check_deps_and_remove/3,
    create/3,
    disable_enable/3,
    get_metrics/2,
    list/0,
    load/0,
    lookup/1,
    lookup/2,
    remove/2,
    unload/0
]).

-export([config_key_path/0]).

%% exported for `emqx_telemetry'
-export([get_basic_usage_info/0]).

%% Data backup
-export([
    import_config/1
]).

-define(ROOT_KEY, connectors).

load() ->
    Connectors = emqx:get_config([?ROOT_KEY], #{}),
    lists:foreach(
        fun({Type, NamedConf}) ->
            lists:foreach(
                fun({Name, Conf}) ->
                    safe_load_connector(Type, Name, Conf)
                end,
                maps:to_list(NamedConf)
            )
        end,
        maps:to_list(Connectors)
    ).

unload() ->
    Connectors = emqx:get_config([?ROOT_KEY], #{}),
    lists:foreach(
        fun({Type, NamedConf}) ->
            lists:foreach(
                fun({Name, _Conf}) ->
                    _ = emqx_connector_resource:stop(Type, Name)
                end,
                maps:to_list(NamedConf)
            )
        end,
        maps:to_list(Connectors)
    ).

safe_load_connector(Type, Name, Conf) ->
    try
        _Res = emqx_connector_resource:create(Type, Name, Conf),
        ?tp(
            emqx_connector_loaded,
            #{
                type => Type,
                name => Name,
                res => _Res
            }
        )
    catch
        Err:Reason:ST ->
            ?SLOG(error, #{
                msg => "load_connector_failed",
                type => Type,
                name => Name,
                error => Err,
                reason => Reason,
                stacktrace => ST
            })
    end.

config_key_path() ->
    [?ROOT_KEY].

pre_config_update([?ROOT_KEY], RawConf, RawConf) ->
    {ok, RawConf};
pre_config_update([?ROOT_KEY], NewConf, _RawConf) ->
    {ok, convert_certs(NewConf)};
pre_config_update(_, {_Oper, _, _}, undefined) ->
    {error, connector_not_found};
pre_config_update(_, {Oper, _Type, _Name}, OldConfig) ->
    %% to save the 'enable' to the config files
    {ok, OldConfig#{<<"enable">> => operation_to_enable(Oper)}};
pre_config_update(Path, Conf, _OldConfig) when is_map(Conf) ->
    case emqx_connector_ssl:convert_certs(filename:join(Path), Conf) of
        {error, Reason} ->
            {error, Reason};
        {ok, ConfNew} ->
            {ok, ConfNew}
    end.

operation_to_enable(disable) -> false;
operation_to_enable(enable) -> true.

post_config_update([?ROOT_KEY], _Req, NewConf, OldConf, _AppEnv) ->
    #{added := Added, removed := Removed, changed := Updated} =
        diff_confs(NewConf, OldConf),
    %% The config update will be failed if any task in `perform_connector_changes` failed.
    Result = perform_connector_changes([
        #{action => fun emqx_connector_resource:remove/4, data => Removed},
        #{
            action => fun emqx_connector_resource:create/3,
            data => Added,
            on_exception_fn => fun emqx_connector_resource:remove/4
        },
        #{action => fun emqx_connector_resource:update/4, data => Updated}
    ]),
    ?tp(connector_post_config_update_done, #{}),
    Result;
post_config_update([?ROOT_KEY, Type, Name], '$remove', _, _OldConf, _AppEnvs) ->
    ok = emqx_connector_resource:remove(Type, Name),
    ?tp(connector_post_config_update_done, #{}),
    ok;
post_config_update([?ROOT_KEY, Type, Name], _Req, NewConf, undefined, _AppEnvs) ->
    ok = emqx_connector_resource:create(Type, Name, NewConf),
    ?tp(connector_post_config_update_done, #{}),
    ok;
post_config_update([connectors, Type, Name], _Req, NewConf, OldConf, _AppEnvs) ->
    ResOpts = emqx_resource:fetch_creation_opts(NewConf),
    ok = emqx_connector_resource:update(Type, Name, {OldConf, NewConf}, ResOpts),
    ?tp(connector_post_config_update_done, #{}),
    ok.

list() ->
    maps:fold(
        fun(Type, NameAndConf, Connectors) ->
            maps:fold(
                fun(Name, RawConf, Acc) ->
                    case lookup(Type, Name, RawConf) of
                        {error, not_found} -> Acc;
                        {ok, Res} -> [Res | Acc]
                    end
                end,
                Connectors,
                NameAndConf
            )
        end,
        [],
        emqx:get_raw_config([connectors], #{})
    ).

lookup(Id) ->
    {Type, Name} = emqx_connector_resource:parse_connector_id(Id),
    lookup(Type, Name).

lookup(Type, Name) ->
    RawConf = emqx:get_raw_config([connectors, Type, Name], #{}),
    lookup(Type, Name, RawConf).

lookup(Type, Name, RawConf) ->
    case emqx_resource:get_instance(emqx_connector_resource:resource_id(Type, Name)) of
        {error, not_found} ->
            {error, not_found};
        {ok, _, Data} ->
            {ok, #{
                type => Type,
                name => Name,
                resource_data => Data,
                raw_config => RawConf
            }}
    end.

get_metrics(Type, Name) ->
    emqx_resource:get_metrics(emqx_connector_resource:resource_id(Type, Name)).

disable_enable(Action, ConnectorType, ConnectorName) when
    Action =:= disable; Action =:= enable
->
    emqx_conf:update(
        config_key_path() ++ [ConnectorType, ConnectorName],
        {Action, ConnectorType, ConnectorName},
        #{override_to => cluster}
    ).

create(ConnectorType, ConnectorName, RawConf) ->
    ?SLOG(debug, #{
        connector_action => create,
        connector_type => ConnectorType,
        connector_name => ConnectorName,
        connector_raw_config => emqx_utils:redact(RawConf)
    }),
    emqx_conf:update(
        emqx_connector:config_key_path() ++ [ConnectorType, ConnectorName],
        RawConf,
        #{override_to => cluster}
    ).

remove(ConnectorType, ConnectorName) ->
    ?SLOG(debug, #{
        brige_action => remove,
        connector_type => ConnectorType,
        connector_name => ConnectorName
    }),
    emqx_conf:remove(
        emqx_connector:config_key_path() ++ [ConnectorType, ConnectorName],
        #{override_to => cluster}
    ).

check_deps_and_remove(ConnectorType, ConnectorName, RemoveDeps) ->
    ConnectorId = emqx_connector_resource:connector_id(ConnectorType, ConnectorName),
    %% NOTE: This violates the design: Rule depends on data-connector but not vice versa.
    case emqx_rule_engine:get_rule_ids_by_action(ConnectorId) of
        [] ->
            remove(ConnectorType, ConnectorName);
        RuleIds when RemoveDeps =:= false ->
            {error, {rules_deps_on_this_connector, RuleIds}};
        RuleIds when RemoveDeps =:= true ->
            lists:foreach(
                fun(R) ->
                    emqx_rule_engine:ensure_action_removed(R, ConnectorId)
                end,
                RuleIds
            ),
            remove(ConnectorType, ConnectorName)
    end.

%%----------------------------------------------------------------------------------------
%% Data backup
%%----------------------------------------------------------------------------------------

import_config(RawConf) ->
    RootKeyPath = config_key_path(),
    ConnectorsConf = maps:get(<<"connectors">>, RawConf, #{}),
    OldConnectorsConf = emqx:get_raw_config(RootKeyPath, #{}),
    MergedConf = merge_confs(OldConnectorsConf, ConnectorsConf),
    case emqx_conf:update(RootKeyPath, MergedConf, #{override_to => cluster}) of
        {ok, #{raw_config := NewRawConf}} ->
            {ok, #{root_key => ?ROOT_KEY, changed => changed_paths(OldConnectorsConf, NewRawConf)}};
        Error ->
            {error, #{root_key => ?ROOT_KEY, reason => Error}}
    end.

merge_confs(OldConf, NewConf) ->
    AllTypes = maps:keys(maps:merge(OldConf, NewConf)),
    lists:foldr(
        fun(Type, Acc) ->
            NewConnectors = maps:get(Type, NewConf, #{}),
            OldConnectors = maps:get(Type, OldConf, #{}),
            Acc#{Type => maps:merge(OldConnectors, NewConnectors)}
        end,
        #{},
        AllTypes
    ).

changed_paths(OldRawConf, NewRawConf) ->
    maps:fold(
        fun(Type, Connectors, ChangedAcc) ->
            OldConnectors = maps:get(Type, OldRawConf, #{}),
            Changed = maps:get(changed, emqx_utils_maps:diff_maps(Connectors, OldConnectors)),
            [[?ROOT_KEY, Type, K] || K <- maps:keys(Changed)] ++ ChangedAcc
        end,
        [],
        NewRawConf
    ).

%%========================================================================================
%% Helper functions
%%========================================================================================

convert_certs(ConnectorsConf) ->
    maps:map(
        fun(Type, Connectors) ->
            maps:map(
                fun(Name, ConnectorConf) ->
                    Path = filename:join([?ROOT_KEY, Type, Name]),
                    case emqx_connector_ssl:convert_certs(Path, ConnectorConf) of
                        {error, Reason} ->
                            ?SLOG(error, #{
                                msg => "bad_ssl_config",
                                type => Type,
                                name => Name,
                                reason => Reason
                            }),
                            throw({bad_ssl_config, Reason});
                        {ok, ConnectorConf1} ->
                            ConnectorConf1
                    end
                end,
                Connectors
            )
        end,
        ConnectorsConf
    ).

perform_connector_changes(Tasks) ->
    perform_connector_changes(Tasks, ok).

perform_connector_changes([], Result) ->
    Result;
perform_connector_changes([#{action := Action, data := MapConfs} = Task | Tasks], Result0) ->
    OnException = maps:get(on_exception_fn, Task, fun(_Type, _Name, _Conf, _Opts) -> ok end),
    Result = maps:fold(
        fun
            ({_Type, _Name}, _Conf, {error, Reason}) ->
                {error, Reason};
            %% for emqx_connector_resource:update/4
            ({Type, Name}, {OldConf, Conf}, _) ->
                ResOpts = emqx_resource:fetch_creation_opts(Conf),
                case Action(Type, Name, {OldConf, Conf}, ResOpts) of
                    {error, Reason} -> {error, Reason};
                    Return -> Return
                end;
            ({Type, Name}, Conf, _) ->
                ResOpts = emqx_resource:fetch_creation_opts(Conf),
                try Action(Type, Name, Conf, ResOpts) of
                    {error, Reason} -> {error, Reason};
                    Return -> Return
                catch
                    Kind:Error:Stacktrace ->
                        ?SLOG(error, #{
                            msg => "connector_config_update_exception",
                            kind => Kind,
                            error => Error,
                            type => Type,
                            name => Name,
                            stacktrace => Stacktrace
                        }),
                        OnException(Type, Name, Conf, ResOpts),
                        erlang:raise(Kind, Error, Stacktrace)
                end
        end,
        Result0,
        MapConfs
    ),
    perform_connector_changes(Tasks, Result).

diff_confs(NewConfs, OldConfs) ->
    emqx_utils_maps:diff_maps(
        flatten_confs(NewConfs),
        flatten_confs(OldConfs)
    ).

flatten_confs(Conf0) ->
    maps:from_list(
        lists:flatmap(
            fun({Type, Conf}) ->
                do_flatten_confs(Type, Conf)
            end,
            maps:to_list(Conf0)
        )
    ).

do_flatten_confs(Type, Conf0) ->
    [{{Type, Name}, Conf} || {Name, Conf} <- maps:to_list(Conf0)].

-spec get_basic_usage_info() ->
    #{
        num_connectors => non_neg_integer(),
        count_by_type =>
            #{ConnectorType => non_neg_integer()}
    }
when
    ConnectorType :: atom().
get_basic_usage_info() ->
    InitialAcc = #{num_connectors => 0, count_by_type => #{}},
    try
        lists:foldl(
            fun
                (#{resource_data := #{config := #{enable := false}}}, Acc) ->
                    Acc;
                (#{type := ConnectorType}, Acc) ->
                    NumConnectors = maps:get(num_connectors, Acc),
                    CountByType0 = maps:get(count_by_type, Acc),
                    CountByType = maps:update_with(
                        binary_to_atom(ConnectorType, utf8),
                        fun(X) -> X + 1 end,
                        1,
                        CountByType0
                    ),
                    Acc#{
                        num_connectors => NumConnectors + 1,
                        count_by_type => CountByType
                    }
            end,
            InitialAcc,
            list()
        )
    catch
        %% for instance, when the connector app is not ready yet.
        _:_ ->
            InitialAcc
    end.
