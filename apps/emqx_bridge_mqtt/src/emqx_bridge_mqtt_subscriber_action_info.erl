%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_mqtt_subscriber_action_info).

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

action_type_name() -> mqtt_subscriber.

connector_type_name() -> mqtt_subscriber.

schema_module() -> emqx_bridge_action_mqtt_subscriber_schema.

bridge_v1_config_to_connector_config(InConfig) ->
    emqx_bridge_mqtt_publisher_action_info:bridge_v1_config_to_connector_config(InConfig).

bridge_v1_config_to_action_config(BridgeV1Config, ConnectorName) ->
    emqx_bridge_mqtt_publisher_action_info:bridge_v1_config_to_action_config(
        BridgeV1Config, ConnectorName
    ).
