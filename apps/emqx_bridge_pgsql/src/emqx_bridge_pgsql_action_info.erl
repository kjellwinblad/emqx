%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_pgsql_action_info).

-behaviour(emqx_action_info).

-export([
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    bridge_v1_to_action_fixup/1
]).

bridge_v1_type_name() -> pgsql.

action_type_name() -> pgsql.

connector_type_name() -> pgsql.

schema_module() -> emqx_bridge_pgsql_schema.

bridge_v1_to_action_fixup(Config) ->
    %% Move sql key to parameters
    SqlField = maps:get(<<"sql">>, Config),
    Config1 = maps:remove(<<"sql">>, Config),
    Config2 =
        emqx_utils_maps:deep_put([<<"parameters">>, <<"sql">>], Config1, SqlField),
    %% Move prepare_statement key to parameters if it exists
    PrepareStatementField = maps:get(<<"prepare_statement">>, Config2, undefined),
    case PrepareStatementField of
        undefined ->
            Config2;
        _ ->
            Config3 = maps:remove(<<"prepare_statement">>, Config2),
            emqx_utils_maps:deep_put(
                [<<"parameters">>, <<"prepare_statement">>], Config3, PrepareStatementField
            )
    end.
