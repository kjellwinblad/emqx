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
    x:show(fixup_config, Config),
    Config.
