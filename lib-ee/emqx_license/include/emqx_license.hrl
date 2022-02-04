%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% @doc EMQ X License Management CLI.
%%--------------------------------------------------------------------

-ifndef(_EMQX_LICENSE_).
-define(_EMQX_LICENSE_, true).

-define(EVALUATION_LOG,
    "\n"
    "===============================================================================\n"
    "This is an evaluation license that is restricted to 10 concurrent connections.\n"
    "If you already have a paid license, please apply it now.\n"
    "Or you could visit https://www.emqx.io/licenses to get a trial license.\n"
    "===============================================================================\n"
    ).

-define(EXPIRY_LOG,
    "\n"
    "======================================================\n"
    "Your license has expired.\n"
    "Please visit https://www.emqx.io/licenses or\n"
    "contact our customer services for an updated license.\n"
    "======================================================\n"
    ).

-define(OFFICIAL, 1).
-define(TRIAL, 0).

-define(SMALL_CUSTOMER, 0).
-define(MEDIUM_CUSTOMER, 1).
-define(LARGE_CUSTOMER, 2).
-define(EVALUATION_CUSTOMER, 10).

-define(EXPIRED_DAY, -90).

-endif.
