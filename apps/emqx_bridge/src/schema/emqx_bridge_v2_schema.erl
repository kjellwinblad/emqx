%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_bridge_v2_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(hoconsc, [mk/2, ref/2, ref/1]).

-export([roots/0, fields/1, desc/1, namespace/0, tags/0]).

-export([
    get_response/0,
    put_request/0,
    post_request/0,
    examples/1
]).

%% Exported for mocking
%% TODO: refactor emqx_bridge_v1_compatibility_layer_SUITE so we don't need to
%% export this
-export([
    registered_api_schemas/1
]).

-export([types/0, types_sc/0]).

-export([make_action_schema/1]).

-export_type([action_type/0]).

%% Should we explicitly list them here so dialyzer may be more helpful?
-type action_type() :: atom().

%%======================================================================================
%% For HTTP APIs
get_response() ->
    api_schema("get").

put_request() ->
    api_schema("put").

post_request() ->
    api_schema("post").

api_schema(Method) ->
    APISchemas = ?MODULE:registered_api_schemas(Method),
    hoconsc:union(bridge_api_union(APISchemas)).

registered_api_schemas(Method) ->
    RegisteredSchemas = emqx_action_info:registered_schema_modules(),
    [
        api_ref(SchemaModule, atom_to_binary(BridgeV2Type), Method ++ "_bridge_v2")
     || {BridgeV2Type, SchemaModule} <- RegisteredSchemas
    ].

api_ref(Module, Type, Method) ->
    {Type, ref(Module, Method)}.

bridge_api_union(Refs) ->
    Index = maps:from_list(Refs),
    fun
        (all_union_members) ->
            maps:values(Index);
        ({value, V}) ->
            case V of
                #{<<"type">> := T} ->
                    case maps:get(T, Index, undefined) of
                        undefined ->
                            throw(#{
                                field_name => type,
                                value => T,
                                reason => <<"unknown bridge type">>
                            });
                        Ref ->
                            [Ref]
                    end;
                _ ->
                    maps:values(Index)
            end
    end.

%%======================================================================================
%% HOCON Schema Callbacks
%%======================================================================================

namespace() -> "actions".

tags() ->
    [<<"Actions">>].

-dialyzer({nowarn_function, roots/0}).

roots() ->
    case fields(actions) of
        [] ->
            [
                {actions,
                    ?HOCON(hoconsc:map(name, typerefl:map()), #{importance => ?IMPORTANCE_LOW})}
            ];
        _ ->
            [{actions, ?HOCON(?R_REF(actions), #{importance => ?IMPORTANCE_LOW})}]
    end.

fields(actions) ->
    registered_schema_fields();
fields(resource_opts) ->
    emqx_resource_schema:create_opts(_Overrides = []).

registered_schema_fields() ->
    [
        begin
            x:show(module_name, Module),
            Module:fields(action)
        end
     || {_BridgeV2Type, Module} <- emqx_action_info:registered_schema_modules()
    ].

desc(actions) ->
    ?DESC("desc_bridges_v2");
desc(_) ->
    undefined.

-spec types() -> [action_type()].
types() ->
    proplists:get_keys(?MODULE:fields(actions)).

-spec types_sc() -> ?ENUM([action_type()]).
types_sc() ->
    hoconsc:enum(types()).

examples(Method) ->
    MergeFun =
        fun(Example, Examples) ->
            maps:merge(Examples, Example)
        end,
    Fun =
        fun(Module, Examples) ->
            ConnectorExamples = erlang:apply(Module, bridge_v2_examples, [Method]),
            lists:foldl(MergeFun, Examples, ConnectorExamples)
        end,
    SchemaModules = [Mod || {_, Mod} <- emqx_action_info:registered_schema_modules()],
    lists:foldl(Fun, #{}, SchemaModules).

%%======================================================================================
%% Helper functions for making HOCON Schema
%%======================================================================================

make_action_schema(ActionParametersRef) ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
        {connector,
            mk(binary(), #{
                desc => ?DESC(emqx_connector_schema, "connector_field"), required => true
            })},
        {description, emqx_schema:description_schema()},
        {local_topic, mk(binary(), #{required => false, desc => ?DESC(mqtt_topic)})},
        {parameters, ActionParametersRef},
        {resource_opts,
            mk(ref(?MODULE, resource_opts), #{default => #{}, desc => ?DESC(resource_opts)})}
    ].

-ifdef(TEST).
-include_lib("hocon/include/hocon_types.hrl").
schema_homogeneous_test() ->
    case
        lists:filtermap(
            fun({_Name, Schema}) ->
                is_bad_schema(Schema)
            end,
            fields(actions)
        )
    of
        [] ->
            ok;
        List ->
            throw(List)
    end.

is_bad_schema(#{type := ?MAP(_, ?R_REF(Module, TypeName))}) ->
    Fields = Module:fields(TypeName),
    ExpectedFieldNames = common_field_names(),
    MissingFileds = lists:filter(
        fun(Name) -> lists:keyfind(Name, 1, Fields) =:= false end, ExpectedFieldNames
    ),
    case MissingFileds of
        [] ->
            false;
        _ ->
            {true, #{
                schema_modle => Module,
                type_name => TypeName,
                missing_fields => MissingFileds
            }}
    end.

common_field_names() ->
    [
        enable, description, local_topic, connector, resource_opts, parameters
    ].

-endif.
