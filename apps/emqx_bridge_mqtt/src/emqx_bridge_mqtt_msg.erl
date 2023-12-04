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

-module(emqx_bridge_mqtt_msg).

-export([parse/1]).
-export([render/2]).

-export_type([msgvars/0]).

-type template() :: emqx_placeholder:tmpl_token().

-type msgvars() :: #{
    topic => template(),
    qos => template() | emqx_types:qos(),
    retain => template() | boolean(),
    payload => template() | undefined
}.

%%

-spec parse(#{
    topic => iodata(),
    qos => iodata() | emqx_types:qos(),
    retain => iodata() | boolean(),
    payload => iodata()
}) ->
    msgvars().
parse(Conf) ->
    Acc1 = parse_field(topic, Conf, Conf),
    Acc2 = parse_field(qos, Conf, Acc1),
    Acc3 = parse_field(payload, Conf, Acc2),
    parse_field(retain, Conf, Acc3).

parse_field(Key, Conf, Acc) ->
    case Conf of
        #{Key := Val} when is_binary(Val) ->
            Acc#{Key => emqx_placeholder:preproc_tmpl(Val)};
        #{Key := Val} ->
            Acc#{Key => Val};
        #{} ->
            Acc
    end.

render(
    Msg,
    #{
        topic := TopicToken,
        qos := QoSToken,
        retain := RetainToken
    } = Vars
) ->
    x:show(render, {Msg, Vars}),
    #{
        topic => render_string(TopicToken, Msg),
        payload => render_payload(Vars, Msg),
        qos => render_simple_var(QoSToken, Msg),
        retain => render_simple_var(RetainToken, Msg)
    }.

render_payload(From, MapMsg) ->
    do_render_payload(maps:get(payload, From, undefined), MapMsg).

do_render_payload(undefined, Msg) ->
    x:show(do_render_undefiend),
    emqx_utils_json:encode(Msg);
do_render_payload(Tks, Msg) ->
    x:show(do_render_tokens),
    render_string(Tks, Msg).

%% Replace a string contains vars to another string in which the placeholders are replace by the
%% corresponding values. For example, given "a: ${var}", if the var=1, the result string will be:
%% "a: 1".
render_string(Tokens, Data) when is_list(Tokens) ->
    emqx_placeholder:proc_tmpl(Tokens, Data, #{return => full_binary});
render_string(Val, _Data) ->
    Val.

%% Replace a simple var to its value. For example, given "${var}", if the var=1, then the result
%% value will be an integer 1.
render_simple_var(Tokens, Data) when is_list(Tokens) ->
    [Var] = emqx_placeholder:proc_tmpl(Tokens, Data, #{return => rawlist}),
    Var;
render_simple_var(Val, _Data) ->
    Val.
