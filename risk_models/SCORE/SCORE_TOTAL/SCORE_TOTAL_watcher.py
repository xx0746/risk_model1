import sys, os
from os import path

if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"D:\bdrisk-model\risk_models")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('max_colwidth', 100)
from risk_models import *
from risk_models.SCORE.SCORE_WH.SCORE_WAREHOUSE import ScoreWh
from risk_models.SCORE.SCORE_TR.SCORE_TRACK import ScoreTr
from risk_models.SCORE.SCORE_TD.SCORE_TRADE import ScoreTd
from risk_models.SCORE.SCORE_ST.SCORE_STOCK import ScoreSt
from risk_models.SCORE.SCORE_SC.SCORE_SUPPLYCHAIN import ScoreSc
from risk_models.SCORE.SCORE_PD.SCORE_PRODUCTION import ScorePd
from risk_models.SCORE.SCORE_FN.SCORE_FINANCE import ScoreFn
from risk_models.SCORE.SCORE_CR.SCORE_CREDIT import ScoreCr

def main():
    if params_global.is_test:
        child_task_id = '1ed78494e4b54b5f8d13521a0d417636'
        # child_task_id = '1'
    else:
        child_task_id = sys.argv[1]
    org_code, params, base_time = read_log_table(child_task_id)
    params = '{"score_weight":[0.2,0.2,0.2,0.2,0.2], "weights":[0.2,0.2,0.2,0.2,0.2]}'
    org_code = '91310000132612172J'
    base_time = '2021-07-02 13:02:30'
    last_res_df = pd.DataFrame()

    score_wh = ScoreWh(org_code, params=params, base_time=base_time, child_task_id=child_task_id)
    score_wh.run_score_wh()
    wh_watcher_df = score_wh.get_df_result()

    score_tr = ScoreTr(org_code, params, base_time, child_task_id)
    score_tr.run_score_track()
    tr_watcher_df = score_tr.get_df_result()

    score_td = ScoreTd(org_code, params, base_time, child_task_id)
    score_td.run_score_trade()
    td_watcher_df = score_td.get_df_result()


    score_st = ScoreSt(org_code, params, base_time, child_task_id)
    score_st.run_score_stock()
    st_watcher_df = score_st.get_df_result()

    score_sc = ScoreSc(org_code, params, base_time, child_task_id)
    score_sc.run_score_supplychain()
    sc_watcher_df = score_sc.get_df_result()

    score_pd = ScorePd(org_code, params, base_time, child_task_id)
    score_pd.run_score_production()
    pd_watcher_df = score_pd.get_df_result()

    score_fn = ScoreFn(org_code, params, base_time, child_task_id)
    score_fn.run_score_fn()
    fn_watcher_df = score_fn.get_df_result()

    score_cr = ScoreCr(org_code, params, base_time, child_task_id)
    score_cr.run_score_cr()
    cr_watcher_df = score_cr.get_df_result()
    last_df = pd.concat([wh_watcher_df, tr_watcher_df, td_watcher_df, st_watcher_df, sc_watcher_df, pd_watcher_df, fn_watcher_df, cr_watcher_df])
    last_df.fillna("0")
    print(last_df)
    # last_res_df.to_csv(r"D:/test.csv")

if __name__ == '__main__':
    main()