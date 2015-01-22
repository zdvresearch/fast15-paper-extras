some errors:




at 23:42, the pvl_MountCompleted has no previous 'init' action.

03/13 23:11:58 RQST PVLS0002 Exiting, function = "pvl_MountCompleted", jobid = "20279653", drive = "102102", arg = "WB1879"
03/13 23:13:16 RQST PVLS0001 Entering, function = "pvl_DismountVolume", arg = "WB187900"
03/13 23:13:16 RQST PVLS0002 Exiting, function = "pvl_DismountVolume", arg = "WB187900"
03/13 23:13:43 RQST PVRS0008 Entering pvr_DismountCart, cartridge = "WB1879", drive = "0"
03/13 23:13:43 DBUG PVRS0379 STK Request:   acs_query_volume: seq= 6131, cart= WB1879
03/13 23:14:09 DBUG PVRS0379 STK Request:   acs_query_volume: seq= 6163, cart= WB1879
03/13 23:14:09 RQST PVRS0009 Exiting pvr_DismountCart, cartridge = "WB1879", drive = "0"
03/13 23:42:36 RQST PVLS0001 Entering, function = "pvl_MountCompleted", jobid = "20279994", drive = "102108", arg = "WB1879"
03/13 23:42:38 RQST PVLS0002 Exiting, function = "pvl_MountCompleted", jobid = "20279994", drive = "102108", arg = "WB1879"
03/13 23:43:57 RQST PVLS0001 Entering, function = "pvl_DismountVolume", arg = "WB187900"
03/13 23:43:57 RQST PVLS0002 Exiting, function = "pvl_DismountVolume", arg = "WB187900"
03/13 23:44:29 RQST PVRS0008 Entering pvr_DismountCart, cartridge = "WB1879", drive = "0"
03/13 23:44:29 DBUG PVRS0379 STK Request:   acs_query_volume: seq= 8205, cart= WB1879
03/13 23:44:46 DBUG PVRS0379 STK Request:   acs_query_volume: seq= 8215, cart= WB1879
03/13 23:44:46 RQST PVRS0009 Exiting pvr_DismountCart, cartridge = "WB1879", drive = "0"
03/14 00:12:17 RQST PVLS0001 Entering, function = "pvl_MountAdd", jobid = "20280290", arg = "WB187900"
03/14 00:12:17 RQST PVLS0002 Exiting, function = "pvl_MountAdd", jobid = "20280290", arg = "WB187900"
03/14 00:12:17 RQST PVRS0026 Entering pvr_CartridgeGetAttrs, cartridge = "WB1879", drive = "0"
03/14 00:12:17 RQST PVRS0027 Exiting pvr_CartridgeGetAttrs, cartridge = "WB1879", drive = "0"
03/14 00:12:17 RQST PVRS0004 Entering pvr_Mount, cartridge = "WB1879", drive = "0"



==================================================================
==== Some examples for 'no mount request before pvl_MountCompleted' errors: 

01/05 20:37:14 RQST PVLS0001 Entering, function = "pvl_MountAdd", jobid = "13952276", arg = "MS036200"
01/05 20:37:14 RQST PVLS0002 Exiting, function = "pvl_MountAdd", jobid = "13952276", arg = "MS036200"
01/05 20:37:22 RQST PVLS0001 Entering, function = "pvl_MountCompleted", jobid = "13952276", drive = "4007", arg = "MS0362"
01/05 20:37:34 RQST PVLS0002 Exiting, function = "pvl_MountCompleted", jobid = "13952276", drive = "4007", arg = "MS0362"
01/05 20:55:25 RQST PVLS0001 Entering, function = "pvl_DismountVolume", arg = "MS036200"
01/05 20:55:25 RQST PVLS0002 Exiting, function = "pvl_DismountVolume", arg = "MS036200"

==================================================================
==== Some examples for 'multiple pvl_MountCOmplete' errors: 

PVRS0379 STK Request:   acs_query_volume: seq= 27843, cart= CB6324
01/02 21:25:42 DBUG PVRS0379 STK Request:   acs_query_mount: seq= 27844, cart= CB6324
01/02 21:25:43 DBUG PVRS0379 STK Request:   acs_mount: seq= 27845, cart= CB6324
01/02 21:29:12 RQST PVLS0001 Entering, function = "pvl_MountCompleted", jobid = "13902817", drive = "101113", arg = "CB6324"
01/02 21:33:21 RQST PVLS0002 Exiting, function = "pvl_MountCompleted", jobid = "13902817", drive = "101113", arg = "CB6324"
01/02 21:33:21 DBUG PVRS0379 STK Request:   acs_query_volume: seq= 28136, cart= CB6324
01/02 21:37:45 WARN PVRS0259 Cartridge not readable in drive, will retry in another drive, cartridge = "CB6324", drive = "101113"
01/02 21:38:35 DBUG PVRS0379 STK Request:   acs_query_volume: seq= 28364, cart= CB6324
01/02 21:38:36 DBUG PVRS0379 STK Request:   acs_query_mount: seq= 28367, cart= CB6324
01/02 21:38:37 DBUG PVRS0379 STK Request:   acs_mount: seq= 28368, cart= CB6324
01/02 21:39:03 RQST PVLS0001 Entering, function = "pvl_MountCompleted", jobid = "13902817", drive = "101102", arg = "CB6324"
01/02 21:39:05 RQST PVLS0002 Exiting, function = "pvl_MountCompleted", jobid = "13902817", drive = "101102", arg = "CB6324"
01/02 21:44:00 RQST PVLS0001 Entering, function = "pvl_DismountVolume", arg = "CB632400"
01/02 21:44:00 RQST PVLS0002 Exiting, function = "pvl_DismountVolume", arg = "CB632400"


01/12 21:20:50 RQST PVLS0001 Entering, function = "pvl_MountCompleted", jobid = "14075423", drive = "101115", arg = "RB5860"
01/12 21:20:52 RQST PVLS0002 Exiting, function = "pvl_MountCompleted", jobid = "14075423", drive = "101115", arg = "RB5860"
01/12 21:24:36 RQST PVLS0001 Entering, function = "pvl_DismountVolume", arg = "RB586000"
01/12 21:24:36 RQST PVLS0002 Exiting, function = "pvl_DismountVolume", arg = "RB586000"
01/12 17:53:12 WARN PVLS0134 Client Cancels All Jobs, jobid = "14072111", arg = "RB5860"
01/12 17:53:12 RQST PVRS0008 Entering pvr_DismountCart, cartridge = "RB5860", drive = "0"
01/12 17:53:12 DBUG PVRS0379 STK Request:   acs_query_volume: seq= 787, cart= RB5860
01/12 17:54:05 RQST PVRS0009 Exiting pvr_DismountCart, cartridge = "RB5860", drive = "0"



01/12 17:49:36 EVNT PVRS0239 Reissuing mount request that was pending when PVR was terminated, cartridge = "WB0475", drive = "0"
01/12 17:49:36 DBUG PVRS0379 STK Request:   acs_query_volume: seq= 36, cart= WB0475
01/12 17:49:49 DBUG PVRS0379 STK Request:   acs_query_mount: seq= 95, cart= WB0475
01/12 17:49:50 DBUG PVRS0379 STK Request:   acs_mount: seq= 96, cart= WB0475
01/12 17:50:03 RQST PVRS0004 Entering pvr_Mount, cartridge = "WB0475", drive = "0"
01/12 17:50:03 RQST PVRS0005 Exiting pvr_Mount, cartridge = "WB0475", drive = "0"
01/12 17:50:03 DBUG PVRS0379 STK Request:   acs_query_volume: seq= 133, cart= WB0475
01/12 17:50:12 DBUG PVRS0379 STK Request:   acs_query_volume: seq= 179, cart= WB0475
01/12 17:50:28 DBUG PVRS0379 STK Request:   acs_query_volume: seq= 346, cart= WB0475
01/12 17:50:31 RQST PVLS0001 Entering, function = "pvl_MountCompleted", jobid = "14072063", drive = "105115", arg = "WB0475"
01/12 17:50:33 RQST PVLS0002 Exiting, function = "pvl_MountCompleted", jobid = "14072063", drive = "105115", arg = "WB0475"
01/12 17:50:53 RQST PVLS0001 Entering, function = "pvl_MountCompleted", jobid = "14072063", drive = "105115", arg = "WB0475"
01/12 17:50:53 RQST PVLS0002 Exiting, function = "pvl_MountCompleted", jobid = "14072063", drive = "105115", arg = "WB0475"
01/12 17:52:14 DBUG CORE3103 Unmatched reply from PVL for volume WB047500, job 14072063
01/12 17:53:15 WARN PVLS0134 Client Cancels All Jobs, jobid = "14072063", drive = "105115", arg = "WB047500"
01/12 17:53:41 RQST PVRS0008 Entering pvr_DismountCart, cartridge = "WB0475", drive = "0"
01/12 17:53:41 DBUG PVRS0379 STK Request:   acs_query_volume: seq= 857, cart= WB0475
01/12 17:55:56 DBUG PVRS0379 STK Request:   acs_query_volume: seq= 1005, cart= WB0475
01/12 17:56:03 RQST PVRS0009 Exiting pvr_DismountCart, cartridge = "WB0475", drive = "0"


