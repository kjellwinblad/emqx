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
    bridge_v1_config_to_action_config/2,
    connector_action_config_to_bridge_v1_config/2
]).

bridge_v1_type_name() -> mqtt.

action_type_name() -> mqtt.

connector_type_name() -> mqtt.

schema_module() -> emqx_bridge_action_mqtt_publisher_schema.

bridge_v1_config_to_connector_config(Config) ->
    %% Transform the egress part to mqtt_publisher connector config
    x:show(before_transform_egress, Config),
    ConnectorConfigMap = make_connector_config_from_bridge_v1_config(Config),
    x:show(after_transform_egress, ConnectorConfigMap),
    {ConnectorConfigMap, mqtt}.

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
    %% Ingress part should be changed to a list
    IngressMap0 = maps:get(<<"ingress">>, Config, #{}),
    %% Move pool_size from ingress to the top level
    PoolSize = maps:get(<<"pool_size">>, IngressMap0, emqx_connector_schema_lib:pool_size(default)),
    IngressMap1 = maps:remove(<<"pool_size">>, IngressMap0),
    ConnectorConfigMap3 = maps:put(<<"pool_size">>, PoolSize, ConnectorConfigMap2),
    ConnectorConfigMap4 =
        case IngressMap1 =:= #{} of
            true ->
                ConnectorConfigMap3;
            false ->
                maps:put(<<"ingress">>, [IngressMap1], ConnectorConfigMap3)
        end,
    ConnectorConfigMap4.

bridge_v1_config_to_action_config(BridgeV1Config, ConnectorName) ->
    bridge_v1_config_to_action_config_helper(
        BridgeV1Config, ConnectorName
    ).

bridge_v1_config_to_action_config_helper(
    #{
        <<"egress">> := EgressMap
    } = _Config,
    _ConnectorName
) when map_size(EgressMap) =:= 0 ->
    none;
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
    {ConfigMap2, mqtt};
bridge_v1_config_to_action_config_helper(
    _Config,
    _ConnectorName
) ->
    none.

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

% check_and_simplify_bridge_v1_config(
%     #{
%         <<"egress">> := EgressMap
%     } = Config
% ) when map_size(EgressMap) =:= 0 ->
%     check_and_simplify_bridge_v1_config(maps:remove(<<"egress">>, Config));
% check_and_simplify_bridge_v1_config(
%     #{
%         <<"ingress">> := IngressMap
%     } = Config
% ) when map_size(IngressMap) =:= 0 ->
%     check_and_simplify_bridge_v1_config(maps:remove(<<"ingress">>, Config));
% check_and_simplify_bridge_v1_config(#{
%     <<"egress">> := _EGressMap,
%     <<"ingress">> := _InGressMap
% }) ->
%     %% We should crash beacuse we don't support upgrading when ingress and egress exist at the same time
%     error(
%         {unsupported_config,
%             <<"Upgrade not supported when ingress and egress exist in the same MQTT bridge. Please divide the egress and ingress part to separate bridges in the configuration.">>}
%     );
% check_and_simplify_bridge_v1_config(SimplifiedConfig) ->
%     SimplifiedConfig.

connector_action_config_to_bridge_v1_config(
    ConnectorConfig, ActionConfig
) ->
    x:show(org_con_conf, ConnectorConfig),
    x:show(org_act_conf, ActionConfig),
    Params = maps:get(<<"parameters">>, ActionConfig, #{}),
    ResourceOptsConnector = maps:get(<<"resource_opts">>, ConnectorConfig, #{}),
    ResourceOptsAction = maps:get(<<"resource_opts">>, ActionConfig, #{}),
    ResourceOpts = maps:merge(ResourceOptsConnector, ResourceOptsAction),
    %% Check the direction of the action
    Direction = maps:get(<<"direction">>, Params, <<"publisher">>),
    Parms2 = maps:remove(<<"direction">>, Params),
    PoolSize = maps:get(<<"pool_size">>, ConnectorConfig, 1),
    Parms3 = maps:put(<<"pool_size">>, PoolSize, Parms2),
    ConnectorConfig2 = maps:remove(<<"pool_size">>, ConnectorConfig),
    BridgeV1Conf0 =
        case Direction of
            <<"publisher">> ->
                #{<<"egress">> => Parms3};
            <<"subscriber">> ->
                #{<<"ingress">> => Parms3}
        end,
    BridgeV1Conf1 = maps:merge(BridgeV1Conf0, ConnectorConfig2),
    BridgeV1Conf2 = BridgeV1Conf1#{
        <<"resource_opts">> => ResourceOpts
    },
    x:show(merged_conf, BridgeV1Conf2),
    BridgeV1Conf2.
