import pandas as pd
# import cx_Oracle
import os
from sqlalchemy import create_engine
import re


EC_USER = "AWT"
# EC_USER = "GENERIC"

AWT_EC_USER = os.environ.get("AWT_EC_USER")
AWT_EC_PASSWD = os.environ.get("AWT_EC_PASSWD")
GENERIC_EC_USER = os.environ.get("GENERIC_EC_USER")
GENERIC_EC_PASSWD = os.environ.get("GENERIC_EC_PASSWD")



def get_data(sql, ec_dict):
    if EC_USER == "AWT":
        print("using ec-user: AWT")
        user = os.environ.get("AWT_EC_USER")
        passwd = os.environ.get("AWT_EC_PASSWD")
    else:
        print("using ec-user: GENERIC")
        user = os.environ.get("GENERIC_TROLL_EC_USER")
        passwd = os.environ.get("GENERIC_TROLL_EC_PASSWD")
    hostname = ec_dict["ec_db_hostname"]
    port = ec_dict["ec_db_port"]
    service_name = ec_dict["ec_db_service"]
    #NB: sqlalchemy
    engine = create_engine(f"oracle://{user}:{passwd}@{hostname}:{port}/{service_name}")
    with engine.connect() as connection:
        df = pd.read_sql(sql, connection)
    return df


def get_full_welltest_join(ec_dict, test_device, date_start, date_end, tz=None):
    """
    Welltests with all relevant tables joined..
    prod_test <-> tdev_result <-> pwel_result <-> wbi_result - including WBI's
    giving a lot of duplicate rows for the first two views to be post processed
    :param db:
    :param test_device:
    :param date_start:
    :param date_end:
    :param tz:
    :return:
    """
    # bandit skip workaround
    sql = f"""
    select a.daytime,
    a.end_date,
    a.duration_hrs,
    a.test_no,
    a.result_no,
    a.status,
    a.test_type,
    a.test_device,
    a.active_wells,
    a.flowing_wells,
    a.primary_wells,
    a.created_by,
    a.rev_text,
b.CODE as testsep_code_real,
b.NET_OIL_RATE_ADJ_SM3PERDAY as TDEV_OIL_ADJ_SM3PERDAY /* avoid dupolicate names with pwel*/,
    b.TOT_WATER_RATE_ADJ_M3PERDAY as TDEV_WATER_ADJ_SM3PERDAY,
b.GAS_RATE_ADJ_SM3PERDAY as TDEV_GAS_ADJ_SM3PERDAY,
b.RECORD_STATUS,
b.TESTSEPARATOR_CODE,
c.CODE as PWEL_CODE,
c.NAME as PWEL_NAME,
c.NODE_CLASS_NAME,
c.SND_LABEL,
c.OFFICIAL_NAME,
c.END_DATE_FULL,
c.WELL_CLASS,
c.OP_FCTY_1_CODE,
c.RESULT_NO as pwel_result_no,
c.CHOKE_SIZE,
c.WH_PRESS_BARG,
c.WH_TEMP_C,
c.WH_USC_PRESS_BARG,
c.WH_USC_TEMP_C,
c.WH_DSC_PRESS_BARG,
c.WH_DSC_TEMP_C,
c.BH_PRESS_BARG,
c.BH_TEMP_C,
c.ANNULUS_PRESS_BARG,
c.DP_CHOKE_BARA,
c.GL_RATE_SM3PERDAY,
c.NET_OIL_RATE_ADJ_SM3PERDAY,
c.GAS_RATE_ADJ_SM3PERDAY,
c.WATERCUT_PCT,
c.TOT_WATER_RATE_ADJ_M3PERDAY,
c.LIQUID_RATE_ADJ_SM3PERDAY,
c.COMMENTS as pwel_comments,
d.SLEEVE_POS,
d.NAME as WBI_NAME,
d.CODE as WBI_CODE
FROM rv_prod_test_result a, rv_tdev_result_1 b, rv_pwel_result_1 c, rv_wbi_result d
WHERE a.production_day >= TO_DATE('{date_start}', 'dd.mm.yyyy')
AND a.production_day <= TO_DATE('{date_end}', 'dd.mm.yyyy')
AND a.status = 'ACCEPTED'
AND a.result_no = b.result_no
AND a.result_no = c.result_no
AND a.result_no = d.result_no
AND b.CODE = '{test_device}'
order by a.result_no desc
"""

    df = get_data(sql, ec_dict)
    print("Preparing and cleaning EC data..")
    print("Forcing all column headers to uppercase")
    df.columns = df.columns.str.upper()
    # df[['ACTIVE_WELLS', 'FLOWING_WELLS', 'PRIMARY_WELLS']] = df[['ACTIVE_WELLS', 'FLOWING_WELLS', 'PRIMARY_WELLS']].apply(lambda ser: ser.apply(lambda x: x.split(', ')))
    #    df = df[df['NET_OIL_RATE_ADJ_SM3PERDAY'].notna()] #removing deduction test results (from pre-EC system tests) not used i.e. the ones without rate
    if not tz:
        tz = "Europe/Oslo"
    df[["DAYTIME", "END_DATE"]] = df[["DAYTIME", "END_DATE"]].applymap(
        lambda x: pd.Timestamp(x, tz=tz)
    )
    # df.sort_values(by=['DAYTIME'], inplace=True)
    df.sort_values(by=["RESULT_NO"], inplace=True, ascending=False)
    df.reset_index(drop=True, inplace=True)

    ### Group by pwel and aggregate lists of wbi's
    df[["ACTIVE_WELLS", "FLOWING_WELLS", "PRIMARY_WELLS"]] = df[
        ["ACTIVE_WELLS", "FLOWING_WELLS", "PRIMARY_WELLS"]
    ].apply(lambda s: s.str.split(pat=", ", expand=False))
    df = df.groupby(["PWEL_CODE", "RESULT_NO"], as_index=False).agg(
        {
            k: (list if k in ("SLEEVE_POS", "WBI_NAME", "WBI_CODE") else "first")
            for k in df.columns
        }
    )
    # df = df.groupby(['PWEL_CODE', 'RESULT_NO'], as_index=False).agg(
    #     {k: (tuple if k in ('SLEEVE_POS', 'WBI_NAME', 'WBI_CODE') else 'first') for k in df.columns})
    df.sort_values(by=["RESULT_NO"], inplace=True, ascending=False)
    df.reset_index(drop=True, inplace=True)

    ### Remove all PWEL rows for non-flowing wells (PWEL name != flowing)
    mask = df.apply(lambda s: s.PWEL_NAME in s.FLOWING_WELLS, axis=1)
    df = df[mask]

    def wbi_code_pattern_from_wellcode(
        wellcode,
    ):  # wbi name regex from wellname (accounting for leading 0 (or not) for number)
        split = wellcode.split("-")
        num = split[-1]
        num = re.sub("^0+(?!$)", "", num)  # clean away potential leading zero
        p = re.compile(f"^{split[0]}-{split[1]}-(0?{num}).+")
        return p

    ### Match all WBI entries to the corresponding PWEL for each row (remove others (WBI's for active wells and other PWELS))
    df_wbi = df[["SLEEVE_POS", "WBI_NAME", "WBI_CODE"]].copy()
    for idx, row in df.iterrows():
        wellcode = row["PWEL_CODE"]
        lst = row["WBI_CODE"]
        enum = [
            (i, s)
            for i, s in enumerate(lst)
            if wbi_code_pattern_from_wellcode(wellcode).match(s)
        ]
        df_wbi.loc[idx, "WBI_CODE"] = [x[1] for x in enum]
        idx_lst = [x[0] for x in enum]
        df_wbi.loc[idx, "SLEEVE_POS"] = [row["SLEEVE_POS"][i] for i in idx_lst]
        df_wbi.loc[idx, "WBI_NAME"] = [row["WBI_NAME"][i] for i in idx_lst]

        ### SORTING WBI's within lists (where each list corresponds to PWEL)
        ## (makes rows more comparable)
        sort_idx = [
            df_wbi.at[idx, "WBI_NAME"].index(x)
            for x in sorted(df_wbi.at[idx, "WBI_NAME"])
        ]
        for col in ("WBI_NAME", "SLEEVE_POS", "WBI_CODE"):
            df_wbi.loc[idx, col] = sorted(
                df_wbi.at[idx, col],
                key=lambda x: sort_idx[df_wbi.at[idx, col].index(x)],
            )

        # for reference:
        # sort_idx = [df_wbi.at[idx, 'WBI_NAME'].index(x) for x in sorted(df_wbi.at[idx, 'WBI_NAME'])]
        # df_wbi.loc[idx, 'SLEEVE_POS'] = sorted(df_wbi.at[idx, 'SLEEVE_POS']
        #                               , key=lambda x: sort_idx[df_wbi.at[idx, 'SLEEVE_POS'].index(x)])

    df.loc[:, df_wbi.columns] = df_wbi.copy()

    ### GROUP EACH RESULT_NO and aggregate all pwel and WBI (lists) to lists
    col_end_prodtest_and_tdev = (
        "TESTSEPARATOR_CODE"  # last colum before pwel and wbi cols
    )
    # test = re.findall("(b\..+)", sql)
    idx_divide = df.columns.to_list().index(col_end_prodtest_and_tdev)
    cols_agg_list = df.columns[idx_divide + 1 :]
    # cols_agg_first =df.columns[:idx_divide+1]
    df = df.groupby(["RESULT_NO"], as_index=False).agg(
        {k: (list if k in cols_agg_list else "first") for k in df.columns}
    )
    df.sort_values(by=["RESULT_NO"], inplace=True, ascending=False)
    df.reset_index(drop=True, inplace=True)

    ### SORTING WBI's within lists (where each list corresponds to PWEL) /NOTE: not needed when already sorted WBI's)
    ## (makes rows more comparable)
    # sort_idx = [[lst.index(x) for x in sorted(lst)] for lst in target.at[idx_target, 'WBI_NAME']]
    # [[sorted(lst, key=lambda x: sort_idx[i][lst.index(x)])] for i, lst in
    #  enumerate(target.at[idx_target, 'WBI_NAME'])]

    ### SORTING items in list of lists for WBI aggregates
    ### Note: WBI-related (list of lists), PWEL-related (list), tdev-related(no aggregate)
    for idx, row in df.iterrows():
        # print(idx,row['FLOWING_WELLS'])
        sort_idx = [row["PWEL_NAME"].index(x) for x in row["FLOWING_WELLS"]]
        # df_sort.at[idx,'PWEL_NAME'] = sorted(row['PWEL_NAME'], key=lambda x: sort_idx[row['PWEL_NAME'].index(x)]) # sort only pwel
        ### Sort all aggregated cols to match Flowing wells (i.e. alphabetical)
        for col in cols_agg_list:
            df.at[idx, col] = sorted(
                row[col], key=lambda x: sort_idx[row[col].index(x)]
            )

    return df



if __name__ == "__main__":

    # with open("config.yml") as stream:
    #     config = yaml.safe_load(stream)
    # cfg = config["TRB_1"]
    test_device = 'TD_TRB_TEST1'
    date_start = "01.08.2021"
    date_end = "01.02.2022"

    ec_dict = {
    "ec_db_hostname": "znw-db1006.statoil.no",
    "ec_db_port": 10001,
    # "ec_db_service": "U168E",
    "ec_db_service": "U168E",
}

    ### Cleaning and restructuring data. Get unique pwel cols with aggregated WBI values and just 'first' for tdev and prod_test
    df = get_full_welltest_join(ec_dict, test_device, date_start, date_end)
    fn = "welltests.json"
    df.to_json(fn)
