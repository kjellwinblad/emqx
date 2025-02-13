%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_conf_schema_tests).

-include_lib("eunit/include/eunit.hrl").

doc_gen_test() ->
    Dir = "tmp",
    ok = filelib:ensure_dir(filename:join("tmp", foo)),
    I18nFile = filename:join(["_build", "test", "lib", "emqx_dashboard", "etc", "i18n.conf.all"]),
    _ = emqx_conf:dump_schema(Dir, emqx_conf_schema, I18nFile),
    ok.
