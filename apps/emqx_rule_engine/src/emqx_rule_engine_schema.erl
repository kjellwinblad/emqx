%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rule_engine_schema).

-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).

-export([ namespace/0
        , roots/0
        , fields/1]).

-export([ validate_sql/1
        ]).

namespace() -> rule_engine.

roots() -> ["rule_engine"].

fields("rule_engine") ->
    [ {ignore_sys_message, sc(boolean(), #{default => true, desc =>
"When set to 'true' (default), rule-engine will ignore messages published to $SYS topics."
    })}
    , {rules, sc(hoconsc:map("id", ref("rules")), #{desc => "The rules", default => #{}})}
    ];

fields("rules") ->
    [ {"sql", sc(binary(),
        #{ desc => """
SQL query to transform the messages.<br>
Example: <code>SELECT * FROM \"test/topic\" WHERE payload.x = 1</code><br>
"""
         , nullable => false
         , validator => fun ?MODULE:validate_sql/1})}
    , {"outputs", sc(hoconsc:array(hoconsc:union(
                                   [ binary()
                                   , ref("builtin_output_republish")
                                   , ref("builtin_output_console")
                                   ])),
        #{ desc => """
A list of outputs of the rule.<br>
An output can be a string that refers to the channel Id of a emqx bridge, or a object
that refers to a function.<br>
There a some built-in functions like \"republish\" and \"console\", and we also support user
provided functions like \"ModuleName:FunctionName\".<br>
The outputs in the list is executed one by one in order.
This means that if one of the output is executing slowly, all of the outputs comes after it will not
be executed until it returns.<br>
If one of the output crashed, all other outputs come after it will still be executed, in the
original order.<br>
If there's any error when running an output, there will be an error message, and the 'failure'
counter of the function output or the bridge channel will increase.
"""
        , default => []
        })}
    , {"enable", sc(boolean(), #{desc => "Enable or disable the rule", default => true})}
    , {"description", sc(binary(), #{desc => "The description of the rule", default => <<>>})}
    ];

fields("builtin_output_republish") ->
    [ {function, sc(republish, #{desc => "Republish the message as a new MQTT message"})}
    , {args, sc(ref("republish_args"),
        #{ desc => """
The arguments of the built-in 'republish' output.<br>
We can use variables in the args.<br>

The variables are selected by the rule. For exmaple, if the rule SQL is defined as following:
<code>
    SELECT clientid, qos, payload FROM \"t/1\"
</code>
Then there are 3 variables available: <code>clientid</code>, <code>qos</code> and
<code>payload</code>. And if we've set the args to:
<code>
    {
        topic = \"t/${clientid}\"
        qos = \"${qos}\"
        payload = \"msg: ${payload}\"
    }
</code>
When the rule is triggered by an MQTT message with payload = \"hello\", qos = 1,
clientid = \"steve\", the rule will republish a new MQTT message to topic \"t/steve\",
payload = \"msg: hello\", and qos = 1.
"""
         , default => #{}
         })}
    ];

fields("builtin_output_console") ->
    [ {function, sc(console, #{desc => "Print the outputs to the console"})}
    %% we may support some args for the console output in the future
    %, {args, sc(map(), #{desc => "The arguments of the built-in 'console' output",
    %    default => #{}})}
    ];

fields("republish_args") ->
    [ {topic, sc(binary(),
        #{ desc =>"""
The target topic of message to be re-published.<br>
Template with variables is allowed, see description of the 'republish_args'.
"""
          , nullable => false
          })}
    , {qos, sc(binary(),
        #{ desc => """
The qos of the message to be re-published.
Template with with variables is allowed, see description of the 'republish_args.<br>
Defaults to ${qos}. If variable ${qos} is not found from the selected result of the rule,
0 is used.
"""
         , default => <<"${qos}">>
         })}
    , {retain, sc(binary(),
        #{ desc => """
The retain flag of the message to be re-published.
Template with with variables is allowed, see description of the 'republish_args.<br>
Defaults to ${retain}. If variable ${retain} is not found from the selected result
of the rule, false is used.
"""
        , default => <<"${retain}">>
        })}
    , {payload, sc(binary(),
        #{ desc => """
The payload of the message to be re-published.
Template with with variables is allowed, see description of the 'republish_args.<br>.
Defaults to ${payload}. If variable ${payload} is not found from the selected result
of the rule, then the string \"undefined\" is used.
"""
         , default => <<"${payload}">>
         })}
    ].

validate_sql(Sql) ->
    case emqx_rule_sqlparser:parse(Sql) of
        {ok, _Result} -> ok;
        {error, Reason} -> {error, Reason}
    end.

sc(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Field) -> hoconsc:ref(?MODULE, Field).
