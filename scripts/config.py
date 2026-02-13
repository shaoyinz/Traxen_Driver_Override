"""
config.py
---------
All shared constants for the iQCruise override extraction pipeline.

Edit CFG values here (or override them in main.py) rather than touching
pipeline logic.
"""

# ---------------------------------------------------------------------------
# iQCMode state machine
# ---------------------------------------------------------------------------
#   0 = Not Available     – system off / not engaged         } MANUAL
#   1 = Available         – system ready but NOT controlling } MANUAL
#   2 = Controlled Entry  – iQC taking control               } iQC ACTIVE
#   3 = Active Control    – iQC in full control              } iQC ACTIVE
#   4 = Hold              – iQC holding speed/torque         } iQC ACTIVE
#   5 = Controlled Exit   – iQC ramping down                 } iQC ACTIVE
#   6 = Throttle Override – driver pushed accelerator          OVERRIDE SIGNAL
#   7 = Fault             – system fault                     } MANUAL

CFG: dict = {
    # -- iQCMode state groups ------------------------------------------------
    "IQCMODE_ACTIVE":            {2., 3., 4., 5.},
    "IQCMODE_THROTTLE_OVERRIDE": 6.,
    "IQCMODE_INACTIVE":          {0., 1., 7.},

    # -- Noise / debounce ----------------------------------------------------
    # Bridge inactive blips shorter than this (seconds) sandwiched between
    # active runs – handles 100 ms logger glitches.
    "MIN_INACTIVE_BRIDGE_S": 2.0,

    # Gaps larger than this (seconds) are treated as hard session breaks.
    "MAX_INTRA_FILE_GAP_S": 30.0,

    # Session must have been running at least this long before the override
    # for the event to be considered valid.
    "MIN_ACTIVE_SESSION_S": 5.0,

    # -- Signal thresholds ---------------------------------------------------
    "ACCEL_PEDAL_THRESHOLD_PCT": 5.0,
    "BRAKE_SWITCH_ON":           1.0,

    # Events below this speed are flagged (not dropped) as possible stops.
    "MIN_SPEED_KPH": 15.0,

    # Look-back window (seconds) when de-duplicating post-throttle-override exits.
    "THROTTLE_OVERRIDE_DEDUP_WINDOW_S": 30.0,

    # -- Context window (seconds before / after override point) -------------
    "PRE_OVERRIDE_S":  10.0,
    "POST_OVERRIDE_S": 10.0,
}

# ---------------------------------------------------------------------------
# Column aliases  (short key -> actual CAN signal name in the parquet)
# ---------------------------------------------------------------------------
COLS: dict[str, str] = {
    "ts":            "Timestamp",

    # iQC system state
    "iqc_mode":      "iQC1.iQCMode",
    "iqc_ctrl_mode": "IQC1.iQCControlMode",
    "iqc_actuator":  "IQC1.iQCActuatorControlMode",
    "iqc_health":    "IQC1.iQCHealth",
    "iqc_preview":   "IQC1.iQCPreviewMode",
    "iqc_alert":     "IQC1.IQCDriverAlert",

    # Primary override signals
    "accel_pedal":   "EEC2_Engine.AccelPedalPos1",
    "brake_sw_eng":  "CCVS1_Engine.BrakeSwitch",

    # CC button signals (for button-press exit detection)
    "cc_enable_cab": "CCVS1_Cab_Controller.CruiseCtrlEnableSwitch",
    "cc_enable_mc":  "CCVS1_ManagementComputer.CruiseCtrlEnableSwitch",
    "cc_set_cab":    "CCVS1_Cab_Controller.CruiseCtrlSetSwitch",
    "cc_resume_cab": "CCVS1_Cab_Controller.CruiseCtrlResumeSwitch",

    # Speed
    "spd_wheel":     "CCVS1_Engine.WheelBasedVehicleSpeed",
    "spd_nav":       "VDS_FC.NavigationBasedVehicleSpeed",

    # Road geometry
    "road_grade":    "IQVS1.iQRoadGrade",
    "road_curvature":"ACC1.RoadCurvature",
    "altitude":      "VDS_FC.Altitude",

    # CIPV – radar
    "cipv_dist":     "ACC1_Radar.DistanceToForwardVehicle",
    "cipv_spd":      "ACC1_Radar.SpeedOfForwardVehicle",
    "cipv_detected": "ACC1_Radar.TargetDetected",
    "cipv_dist_iqc": "iQCU_Status_1.DistanceToForwardVehicle",
    "cipv_spd_iqc":  "iQCU_Status_1.Speed2D_FWVehicle",

    # Retarder
    "retarder_torq": "ERC1_Retarder.ActualRetarderPercentTorque",

    # Engine / torque
    "eng_speed":     "EEC1_Engine.EngSpeed",
    "actual_torq":   "EEC1_Engine.ActualEngPercentTorque",
    "demand_torq":   "EEC1_Engine.DriversDemandEngPercentTorque",

    # EH / localization
    "eh_not_local":  "EH_not_localized.EH_not_localized",

    # GPS
    "lat":           "VP_RP_80.Latitude",
    "lon":           "VP_RP_80.Longitude",

    # GVW
    "gvw_est":       "IQVLPE1.iQGrossVehicleWeightEst",
    "gvw_raw":       "CVW.GrossCombinationVehicleWeight",

    # Misc
    "gear":          "ETC2_Transmission.TransCurrentGear",
    "yaw_rate":      "ARI_RP.YawRateExRange",
}

# ---------------------------------------------------------------------------
# Columns written into per-event context window CSVs (stage 9)
# ---------------------------------------------------------------------------
CONTEXT_COLS: list[str] = [
    "Timestamp", "iqcmode_clean",
    "EEC2_Engine.AccelPedalPos1",
    "CCVS1_Engine.BrakeSwitch",
    "CCVS1_Engine.WheelBasedVehicleSpeed",
    "VDS_FC.NavigationBasedVehicleSpeed",
    "EEC1_Engine.EngSpeed",
    "EEC1_Engine.ActualEngPercentTorque",
    "EEC1_Engine.DriversDemandEngPercentTorque",
    "ERC1_Retarder.ActualRetarderPercentTorque",
    "ACC1_Radar.DistanceToForwardVehicle",
    "ACC1_Radar.SpeedOfForwardVehicle",
    "ACC1_Radar.TargetDetected",
    "iQCU_Status_1.DistanceToForwardVehicle",
    "iQCU_Status_1.Speed2D_FWVehicle",
    "IQVS1.iQRoadGrade",
    "ACC1.RoadCurvature",
    "VDS_FC.Altitude",
    "ARI_RP.YawRateExRange",
    "IQCU_IMU_Status_4.PathCurvature",
    "ETC2_Transmission.TransCurrentGear",
    "IQVLPE1.iQGrossVehicleWeightEst",
    "VP_RP_80.Latitude",
    "VP_RP_80.Longitude",
    "EH_not_localized.EH_not_localized",
    "CCVS1_Cab_Controller.CruiseCtrlEnableSwitch",
]

# ---------------------------------------------------------------------------
# Columns written into the final events CSV (stage 10)
# ---------------------------------------------------------------------------
EXPORT_COLS: list[str] = [
    "override_ts", "override_type", "is_noisy", "is_throttle_exit_dup",
    "raw_detection_type", "prev_mode", "cur_mode",
    "speed_kph_at_ovrd", "avg_speed_pre_kph", "avg_speed_post_kph",
    "max_accel_pre_pct", "brake_active_pre", "cc_btn_change_pre",
    "cipv_dist_m", "cipv_detected",
    "road_grade_pct", "road_curvature", "altitude_m",
    "retarder_pct_pre", "gvw_kg",
    "session_dur_s_at_ovrd", "preceding_session_id",
    "eh_not_localized", "lat", "lon", "override_idx",
]
