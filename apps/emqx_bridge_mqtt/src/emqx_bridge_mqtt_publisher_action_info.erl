%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_mqtt_publisher_action_info).

-behaviour(emqx_action_info).

-export([
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    bridge_v1_config_to_connector_config/1,
    bridge_v1_config_to_action_config/2
]).

bridge_v1_type_name() -> mqtt.

action_type_name() -> mqtt.

connector_type_name() -> mqtt.

schema_module() -> emqx_bridge_action_mqtt_publisher_schema.

bridge_v1_config_to_connector_config(Config) ->
    bridge_v1_config_to_connector_config_helper(check_and_simplify_bridge_v1_config(Config)).

bridge_v1_config_to_connector_config_helper(
    #{
        <<"egress">> := _EgressMap
    } = Config
) ->
    %% Transform the egress part to mqtt_publisher connector config
    x:show(before_transform_egress, Config),
    ConnectorConfigMap = make_connector_config_from_bridge_v1_config(Config),
    x:show(after_transform_egress, ConnectorConfigMap),
    {ConnectorConfigMap, mqtt}.
% ;
% bridge_v1_config_to_connector_config_helper(
%     #{
%         <<"ingress">> := _IngressMap
%     } = Config
% ) ->
%     x:show(before_transform_ingress, Config),
%     ConnectorConfigMap = make_connector_config_from_bridge_v1_config(Config),
%     {ConnectorConfigMap, mqtt_subscriber}.

make_connector_config_from_bridge_v1_config(Config) ->
    ConnectorConfigSchema = emqx_bridge_mqtt_connector_schema:fields("config_connector"),
    ConnectorTopFields = [
        erlang:atom_to_binary(FieldName, utf8)
     || {FieldName, _} <- ConnectorConfigSchema
    ],
    ConnectorConfigMap = maps:with(ConnectorTopFields, Config),
    ResourceOptsSchema = emqx_bridge_mqtt_connector_schema:fields(creation_opts),
    ResourceOptsTopFields = [
        erlang:atom_to_binary(FieldName, utf8)
     || {FieldName, _} <- ResourceOptsSchema
    ],
    ResourceOptsMap = maps:get(<<"resource_opts">>, ConnectorConfigMap, #{}),
    ResourceOptsMap2 = maps:with(ResourceOptsTopFields, ResourceOptsMap),
    ConnectorConfigMap2 = maps:put(<<"resource_opts">>, ResourceOptsMap2, ConnectorConfigMap),
    ConnectorConfigMap2.

bridge_v1_config_to_action_config(BridgeV1Config, ConnectorName) ->
    bridge_v1_config_to_action_config_helper(
        check_and_simplify_bridge_v1_config(BridgeV1Config), ConnectorName
    ).

bridge_v1_config_to_action_config_helper(
    #{
        <<"egress">> := EgressMap
    } = Config,
    ConnectorName
) ->
    %% Transform the egress part to mqtt_publisher connector config
    SchemaFields = emqx_bridge_action_mqtt_publisher_schema:fields("mqtt_publisher_action"),
    ResourceOptsSchemaFields = emqx_bridge_action_mqtt_publisher_schema:fields("resource_opts"),
    ConfigMap1 = general_action_conf_map_from_bridge_v1_config(
        Config, ConnectorName, SchemaFields, ResourceOptsSchemaFields
    ),
    %% Add parameters field (Egress map) to the action config
    ConfigMap2 = maps:put(<<"parameters">>, EgressMap, ConfigMap1),
    {ConfigMap2, mqtt}.
% bridge_v1_config_to_action_config_helper(
%     #{
%         <<"ingress">> := IngressMap
%     } = Config,
%     ConnectorName
% ) ->
%     %% Transform the egress part to mqtt_publisher connector config
%     SchemaFields = emqx_bridge_action_mqtt_publisher_schema:fields("mqtt_publisher_action"),
%     ResourceOptsSchemaFields = emqx_bridge_action_mqtt_publisher_schema:fields("resource_opts"),
%     ConfigMap1 = general_action_conf_map_from_bridge_v1_config(
%         Config, ConnectorName, SchemaFields, ResourceOptsSchemaFields
%     ),
%     %% Add parameters field to the action config
%     ConfigMap2 = maps:put(<<"parameters">>, IngressMap, ConfigMap1),
%     {ConfigMap2, mqtt_subscriber}.

general_action_conf_map_from_bridge_v1_config(
    Config, ConnectorName, SchemaFields, ResourceOptsSchemaFields
) ->
    ShemaFieldsNames = [
        erlang:atom_to_binary(FieldName, utf8)
     || {FieldName, _} <- SchemaFields
    ],
    ActionConfig0 = maps:with(ShemaFieldsNames, Config),
    ResourceOptsSchemaFieldsNames = [
        erlang:atom_to_binary(FieldName, utf8)
     || {FieldName, _} <- ResourceOptsSchemaFields
    ],
    ResourceOptsMap = maps:get(<<"resource_opts">>, ActionConfig0, #{}),
    ResourceOptsMap2 = maps:with(ResourceOptsSchemaFieldsNames, ResourceOptsMap),
    %% Only put resource_opts if the original config has it
    ActionConfig1 =
        case maps:is_key(<<"resource_opts">>, ActionConfig0) of
            true ->
                maps:put(<<"resource_opts">>, ResourceOptsMap2, ActionConfig0);
            false ->
                ActionConfig0
        end,
    ActionConfig2 = maps:put(<<"connector">>, ConnectorName, ActionConfig1),
    ActionConfig2.

check_and_simplify_bridge_v1_config(
    #{
        <<"egress">> := EgressMap
    } = Config
) when map_size(EgressMap) =:= 0 ->
    check_and_simplify_bridge_v1_config(maps:remove(<<"egress">>, Config));
check_and_simplify_bridge_v1_config(
    #{
        <<"ingress">> := IngressMap
    } = Config
) when map_size(IngressMap) =:= 0 ->
    check_and_simplify_bridge_v1_config(maps:remove(<<"ingress">>, Config));
check_and_simplify_bridge_v1_config(#{
    <<"egress">> := _EGressMap,
    <<"ingress">> := _InGressMap
}) ->
    %% We should crash beacuse we don't support upgrading when ingress and egress exist at the same time
    error(
        {unsupported_config,
            <<"Upgrade not supported when ingress and egress exist in the same MQTT bridge. Please divide the egress and ingress part to separate bridges in the configuration.">>}
    );
check_and_simplify_bridge_v1_config(SimplifiedConfig) ->
    SimplifiedConfig.

connector_action_config_to_bridge_v1_config(
    ConnectorConfig, ActionConfig
) ->
    ConnectorConfig2 = maps:without(ConnectorConfig, [<<"resource_opts">>, <<"connector">>]),
    ActionConfig2 = maps:without(ActionConfig, [<<"resource_opts">>, <<"connector">>]),
    #{
        <<"egress">> := ConnectorConfig2,
        <<"ingress">> := ActionConfig2
    }.
