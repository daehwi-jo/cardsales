package main

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"charlie/cls"
)

// 수집결과 정상건수 조회
func getResultCnt(bsDt, restID, serID string) (int, int) {
	var statement string
	var rows *sql.Rows
	var err error

	if len(restID) == 0 {
		statement = "select COUNT(a.BIZ_NUM), SUM(IF(RIGHT(a.MOD_DT,6) > DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 10 MINUTE), '%H%i%s') || RIGHT(a.REG_DT,6) > DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 10 MINUTE), '%H%i%s'),1,0)) " +
			"from cc_sync_inf a, cc_comp_inf b where a.BS_DT=? and a.ERR_CD=? and a.BIZ_NUM = b.BIZ_NUM and b.SER_ID=?"
		rows, err = cls.QueryDBbyParam(statement, bsDt, "0000", serID)
	} else {
		statement = "select COUNT(a.BIZ_NUM), 0 " +
			"from cc_sync_inf a, cc_comp_inf b where a.BS_DT=? and a.ERR_CD=? and a.BIZ_NUM = b.BIZ_NUM and b.REST_ID=?"
		rows, err = cls.QueryDBbyParam(statement, bsDt, "0000", restID)
	}

	if err != nil {
		lprintf(1, "[ERROR] getCompInfo: cls.QueryDBbyParam error(%s) \n", err.Error())
		return 0, 0
	}
	defer rows.Close()

	var sum, cnt int
	for rows.Next() {
		err := rows.Scan(&sum, &cnt)
		if err != nil {
			lprintf(1, "[ERROR] getCompInfo: rows.Scan error(%s) \n", err.Error())
			return 0, 0
		}
	}

	return sum, cnt
}

// 여신데이터 수집대상 가맹점 리스트 조회
func getCompInfos(serID, bsDt string) []CompInfoType {
	statement := "select a.BIZ_NUM, a.SVC_OPEN_DT, a.LN_FIRST_YN, a.LN_JOIN_TY, a.LN_ID, a.LN_PSW, a.LN_JOIN_STS_CD, " +
		"IFNULL(b.BS_DT,'') as BS_DT, IFNULL(left(b.REG_DT,8),'') AS REG_DT, IFNULL(left(b.MOD_DT,8),'') AS MOD_DT, IFNULL(b.STS_CD,'') as STS_CD, IFNULL(b.ERR_CD,'') as ERRCD " +
		"from cc_comp_inf a left join cc_sync_inf b on a.BIZ_NUM=b.BIZ_NUM and b.BS_DT=? " +
		"where a.SER_ID=? and a.COMP_STS_CD=? and a.LN_JOIN_STS_CD=?;"

	rows, err := cls.QueryDBbyParam(statement, bsDt, serID, "1", "1") // 여신협회가입상태, 금결원가입상태
	if err != nil {
		lprintf(1, "[ERROR] getCompInfo: cls.QueryDBbyParam error(%s) \n", err.Error())
		return nil
	}
	defer rows.Close()

	var compInfos []CompInfoType
	for rows.Next() {
		var compInfo CompInfoType
		err := rows.Scan(&compInfo.BizNum, &compInfo.SvcOpenDt, &compInfo.LnFirstYn, &compInfo.LnJoinTy, &compInfo.LnID, &compInfo.LnPsw, &compInfo.LnJoinStsCd, &compInfo.BsDt, &compInfo.RegDt, &compInfo.ModDt, &compInfo.StsCd, &compInfo.ErrCd)
		if err != nil {
			lprintf(1, "[ERROR] getCompInfo: rows.Scan error(%s) \n", err.Error())
			return nil
		}
		compInfos = append(compInfos, compInfo)
	}

	return compInfos
}

// 여신데이터 수집대상 가맹점 신규 리스트 조회
func getCompInfosNew(serID, bsDt, openDt string) []CompInfoType {
	statement := "select a.BIZ_NUM, a.SVC_OPEN_DT, a.LN_FIRST_YN, a.LN_JOIN_TY, a.LN_ID, a.LN_PSW, a.LN_JOIN_STS_CD, " +
		"IFNULL(b.BS_DT,'') as BS_DT, IFNULL(left(b.REG_DT,8),'') AS REG_DT, IFNULL(left(b.MOD_DT,8),'') AS MOD_DT, IFNULL(b.STS_CD,'') as STS_CD, IFNULL(b.ERR_CD,'') as ERRCD " +
		"from cc_comp_inf a left join cc_sync_inf b on a.BIZ_NUM=b.BIZ_NUM and b.BS_DT=? " +
		"where a.SER_ID=? and a.COMP_STS_CD=? and a.LN_JOIN_STS_CD=? and a.SVC_OPEN_DT=?;"

	rows, err := cls.QueryDBbyParam(statement, bsDt, serID, "1", "1", openDt) // 여신협회가입상태, 금결원가입상태, 가입일자
	if err != nil {
		lprintf(1, "[ERROR] getCompInfo: cls.QueryDBbyParam error(%s) \n", err.Error())
		return nil
	}
	defer rows.Close()

	var compInfos []CompInfoType
	for rows.Next() {
		var compInfo CompInfoType
		err := rows.Scan(&compInfo.BizNum, &compInfo.SvcOpenDt, &compInfo.LnFirstYn, &compInfo.LnJoinTy, &compInfo.LnID, &compInfo.LnPsw, &compInfo.LnJoinStsCd, &compInfo.BsDt, &compInfo.RegDt, &compInfo.ModDt, &compInfo.StsCd, &compInfo.ErrCd)
		if err != nil {
			lprintf(1, "[ERROR] getCompInfo: rows.Scan error(%s) \n", err.Error())
			return nil
		}
		compInfos = append(compInfos, compInfo)
	}

	return compInfos
}

// 지정가맹점 여신데이터 수집 정보조회
func getCompInfosByRestID(restID, bsDt string) []CompInfoType {
	statement := "select a.BIZ_NUM, a.SVC_OPEN_DT, a.LN_FIRST_YN, a.LN_JOIN_TY, a.LN_ID, a.LN_PSW, a.LN_JOIN_STS_CD, " +
		"IFNULL(b.BS_DT,'') as BS_DT, IFNULL(left(b.REG_DT,8),'') AS REG_DT, IFNULL(left(b.MOD_DT,8),'') AS MOD_DT, IFNULL(b.STS_CD,'') as STS_CD, IFNULL(b.ERR_CD,'') as ERRCD " +
		"from cc_comp_inf a left join cc_sync_inf b on a.BIZ_NUM=b.BIZ_NUM and b.BS_DT=? " +
		"where a.REST_ID=? and a.COMP_STS_CD=? and a.LN_JOIN_STS_CD=?;"

	rows, err := cls.QueryDBbyParam(statement, bsDt, restID, "1", "1") // 여신협회가입상태, 금결원가입상태
	if err != nil {
		lprintf(1, "[ERROR] getCompInfo: cls.QueryDBbyParam error(%s) \n", err.Error())
		return nil
	}
	defer rows.Close()

	var compInfos []CompInfoType
	for rows.Next() {
		var compInfo CompInfoType
		err := rows.Scan(&compInfo.BizNum, &compInfo.SvcOpenDt, &compInfo.LnFirstYn, &compInfo.LnJoinTy, &compInfo.LnID, &compInfo.LnPsw, &compInfo.LnJoinStsCd, &compInfo.BsDt, &compInfo.RegDt, &compInfo.ModDt, &compInfo.StsCd, &compInfo.ErrCd)
		if err != nil {
			lprintf(1, "[ERROR] getCompInfo: rows.Scan error(%s) \n", err.Error())
			return nil
		}
		compInfos = append(compInfos, compInfo)
	}

	return compInfos
}

// 지정 신규 가맹점 여신데이터 수집 정보조회
func getCompInfosByRestIDNew(restID, bsDt, openDt string) []CompInfoType {
	statement := "select a.BIZ_NUM, a.SVC_OPEN_DT, a.LN_FIRST_YN, a.LN_JOIN_TY, a.LN_ID, a.LN_PSW, a.LN_JOIN_STS_CD, " +
		"IFNULL(b.BS_DT,'') as BS_DT, IFNULL(left(b.REG_DT,8),'') AS REG_DT, IFNULL(left(b.MOD_DT,8),'') AS MOD_DT, IFNULL(b.STS_CD,'') as STS_CD, IFNULL(b.ERR_CD,'') as ERRCD " +
		"from cc_comp_inf a left join cc_sync_inf b on a.BIZ_NUM=b.BIZ_NUM and b.BS_DT=? " +
		"where a.REST_ID=? and a.COMP_STS_CD=? and a.LN_JOIN_STS_CD=? and a.SVC_OPEN_DT=?;"

	rows, err := cls.QueryDBbyParam(statement, bsDt, restID, "1", "1", openDt) // 여신협회가입상태, 금결원가입상태
	if err != nil {
		lprintf(1, "[ERROR] getCompInfo: cls.QueryDBbyParam error(%s) \n", err.Error())
		return nil
	}
	defer rows.Close()

	var compInfos []CompInfoType
	for rows.Next() {
		var compInfo CompInfoType
		err := rows.Scan(&compInfo.BizNum, &compInfo.SvcOpenDt, &compInfo.LnFirstYn, &compInfo.LnJoinTy, &compInfo.LnID, &compInfo.LnPsw, &compInfo.LnJoinStsCd, &compInfo.BsDt, &compInfo.RegDt, &compInfo.ModDt, &compInfo.StsCd, &compInfo.ErrCd)
		if err != nil {
			lprintf(1, "[ERROR] getCompInfo: rows.Scan error(%s) \n", err.Error())
			return nil
		}
		compInfos = append(compInfos, compInfo)
	}

	return compInfos
}

// 여신협회자료받기최초실행여부 업데이트
func updateCompInfo(goID int, bizNum string) int {
	statememt := "update cc_comp_inf set LN_FIRST_YN='Y' where BIZ_NUM=?;"

	var params []interface{}
	params = append(params, bizNum)
	ret, err := cls.ExecDBbyParam(statememt, params)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.ExecDBbyParam error(%s) \n", goID, err.Error())
		return -1
	}

	return ret
}

// 가맹점 Sync 데이터 조회
func selectSync(goID int, bizNum, startDt, endDt string) map[string]SyncInfoType {
	statement := "select BIZ_NUM, BS_DT, SITE_CD, APRV_CNT, APRV_AMT, PCA_CNT, PCA_AMT, PAY_CNT, PAY_AMT, IFNULL(REG_DT,'') as REG_DT, IFNULL(MOD_DT,'') as MOD_DT, STS_CD, ERR_CD from cc_sync_inf where BIZ_NUM=? and BS_DT >= ? and BS_DT <= ? and SITE_CD='1' order by BS_DT desc;"

	rows, err := cls.QueryDBbyParam(statement, bizNum, startDt, endDt)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] selectSync: cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return nil
	}
	defer rows.Close()

	var syncInfos map[string]SyncInfoType
	syncInfos = make(map[string]SyncInfoType)
	for rows.Next() {
		var syncInfo SyncInfoType
		err := rows.Scan(&syncInfo.BizNum, &syncInfo.BsDt, &syncInfo.SiteCd, &syncInfo.AprvCnt, &syncInfo.AprvAmt, &syncInfo.PcaCnt, &syncInfo.PcaAmt, &syncInfo.PayCnt, &syncInfo.PayAmt, &syncInfo.RegDt, &syncInfo.ModDt, &syncInfo.StsCd, &syncInfo.ErrCd)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] selectSync: rows.Scan error(%s) \n", goID, err.Error())
			return nil
		}
		syncInfos[syncInfo.BsDt] = syncInfo
	}

	return syncInfos
}

// Sync 결과 DB 저장
func insertSync(goID int, syncData SyncInfoType) int {
	var params []interface{}
	var fields []string
	var wheres []string
	var inserts []string
	var statement string

	// SYNC 결과데이터 DB조회
	statement = "select BIZ_NUM, BS_DT, SITE_CD, APRV_CNT, APRV_AMT, PCA_CNT, PCA_AMT, PAY_CNT, PAY_AMT, IFNULL(REG_DT,'') as REG_DT, IFNULL(MOD_DT,'') as MOD_DT, STS_CD, ERR_CD from cc_sync_inf where BIZ_NUM=? and BS_DT=? and SITE_CD=?;"

	rows, err := cls.QueryDBbyParam(statement, syncData.BizNum, syncData.BsDt, "1")
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return -1
	}
	defer rows.Close()

	var syncInfo SyncInfoType
	for rows.Next() {
		err := rows.Scan(&syncInfo.BizNum, &syncInfo.BsDt, &syncInfo.SiteCd, &syncInfo.AprvCnt, &syncInfo.AprvAmt, &syncInfo.PcaCnt, &syncInfo.PcaAmt, &syncInfo.PayCnt, &syncInfo.PayAmt, &syncInfo.RegDt, &syncInfo.ModDt, &syncInfo.StsCd, &syncInfo.ErrCd)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] rows.Scan error(%s) \n", goID, err.Error())
			return -1
		}
	}
	lprintf(4, "[INFO][go-%d] syncInfo (%v) \n", goID, syncInfo)

	if syncData.AprvAmt == syncInfo.AprvAmt && syncData.AprvCnt == syncInfo.AprvCnt &&
		syncData.PayAmt == syncInfo.PayAmt && syncData.PayCnt == syncInfo.PayCnt &&
		syncData.PcaAmt == syncInfo.PcaAmt && syncData.PcaCnt == syncInfo.PcaCnt && syncData.ErrCd == syncInfo.ErrCd {
		// 값이나 조회 결과의 변동은 없으면 send_dt 설정 안함
		lprintf(4, "[INFO][go-%d] result success but there is not any change (%v) \n", goID, syncInfo)
		syncData.SendDt = ""
	}

	// Sync 데이터 저장/업데이트
	elements := reflect.ValueOf(&syncData).Elem()
	if len(syncInfo.BizNum) == 0 {
		for k := 0; k < elements.NumField(); k++ {
			mValue := elements.Field(k)
			value := fmt.Sprint(mValue.Interface())
			if len(value) > 0 {
				mType := elements.Type().Field(k)
				tag := mType.Tag

				fields = append(fields, tag.Get("db"))
				inserts = append(inserts, "?")
				params = append(params, value)
			}
		}

		// "insert into cc_sync_inf (BIZ_NUM, BS_DT, SITE_CD, APRV_CNT, PCA_CNT, PAY_CNT, REG_DTM, STS_CD, ERR_CD) values (?,?,?,?,?,?,?,?,?)"
		statement = "insert into cc_sync_inf (" + strings.Join(fields, ", ") + ") values (" + strings.Join(inserts, ", ") + ")"
	} else if syncData.ErrCd != "0000" && syncInfo.ErrCd == "0000" {
		// 정상이였던 데이터를 재수집 하다가 에러가 난 경우 sync update 안함
		lprintf(4, "[INFO][go-%d] result fail but earlier result was success -> do not change (%v) \n", goID, syncInfo)
		return -1
	} else {
		var params2 []interface{}
		for k := 0; k < elements.NumField(); k++ {
			mValue := elements.Field(k)
			value := fmt.Sprint(mValue.Interface())
			if len(value) > 0 {
				mType := elements.Type().Field(k)
				tag := mType.Tag

				field := tag.Get("db")
				if len(field) > 0 {
					if strings.Compare(field, "BIZ_NUM") == 0 || strings.Compare(field, "BS_DT") == 0 || strings.Compare(field, "SITE_CD") == 0 {
						wheres = append(wheres, fmt.Sprint(field, "=?"))
						params2 = append(params2, value)
					} else if strings.Compare(field, "APRV_CNT") == 0 || strings.Compare(field, "APRV_AMT") == 0 ||
						strings.Compare(field, "PCA_CNT") == 0 || strings.Compare(field, "PCA_AMT") == 0 ||
						strings.Compare(field, "PAY_CNT") == 0 || strings.Compare(field, "PAY_AMT") == 0 ||
						strings.Compare(field, "STS_CD") == 0 || strings.Compare(field, "ERR_CD") == 0 ||
						strings.Compare(field, "SEND_DT") == 0 {
						fields = append(fields, fmt.Sprint(field, "=?"))
						params = append(params, value)
					} else {
						continue
					}
				}
				// fmt.Printf("%10s:%10s=%10v, db: %10s\n",
				// 	mType.Name, mType.Type, mValue.Interface(), tag.Get("db")) // 이름, 타입, 값, 태그
			}
		}
		fields = append(fields, "MOD_DT=?")
		params = append(params, time.Now().Format("20060102150405"))

		for _, p := range params2 {
			params = append(params, p)
		}

		// "update cc_sync_inf set APRV_CNT=?, APRV_AMT=?, PCA_CNT=?, PCA_AMT=?, PAY_CNT=?, PAY_AMT=?, MOD_DT=?, STS_CD=?, ERR_CODE=? where BIZ_NUM=? and BS_DT=? and SITE_CD=?"
		statement = "update cc_sync_inf set " + strings.Join(fields, ", ") + " where " + strings.Join(wheres, " and ")
	}

	ret, err := cls.ExecDBbyParam(statement, params)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.ExecDBbyParam error(%s) \n", goID, err.Error())
		return -1
	}

	return ret

}

// 가맹점 Sync 데이터 삭제
func deleteSync(goID int, bizNum, bsDt string) int {
	statement := "delete from cc_sync_inf where BIZ_NUM=? and BS_DT=? and SITE_CD='1';"

	var params []interface{}
	params = append(params, bizNum)
	params = append(params, bsDt)
	ret, err := cls.ExecDBbyParam(statement, params)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.ExecDBbyParam error(%s) \n", goID, err.Error())
		return -1
	}

	return ret
}

// 승인 데이터 조회
func selectApprSum(goID int, bizNum, bsDt string) *ResultSumType {
	statement := "select TOT_CNT, TOT_AMT, APRV_CNT, APRV_AMT, CAN_CNT, CAN_AMT from cc_aprv_sum where BIZ_NUM=? and BS_DT=?;"

	rows, err := cls.QueryDBbyParam(statement, bizNum, bsDt)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] selectApprSumData: cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return nil
	}
	defer rows.Close()

	resultSum := new(ResultSumType)
	for rows.Next() {
		err := rows.Scan(&resultSum.TotTrnsCnt, &resultSum.TotTrnsAmt, &resultSum.TotAuthCnt, &resultSum.TotAuthAmt, &resultSum.TotCnclCnt, &resultSum.TotCnclAmt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] selectApprSumData: rows.Scan error(%s) \n", goID, err.Error())
			return nil
		}
	}

	return resultSum
}

// 매입 데이터 조회
func selectPcaSum(goID int, bizNum, bsDt string) *ResultPurSumType {
	statement := "select PCA_CNT, PCA_AMT, MER_FEE, PNT_FEE, ETC_FEE, TOT_FEE, VAT_AMT, OUTP_EXPT_AMT from cc_pca_sum where BIZ_NUM=? and BS_DT=?;"

	rows, err := cls.QueryDBbyParam(statement, bizNum, bsDt)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] selectPcaSumData: cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return nil
	}
	defer rows.Close()

	resultSum := new(ResultPurSumType)
	for rows.Next() {
		err := rows.Scan(&resultSum.PcaCnt, &resultSum.PcaScdAmt, &resultSum.MerFee, &resultSum.PntFee, &resultSum.EtcFee, &resultSum.TotFee, &resultSum.VatAmt, &resultSum.OuptExptAmt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] selectPcaSumData: rows.Scan error(%s) \n", goID, err.Error())
			return nil
		}
	}

	return resultSum
}

// 입금 데이터 조회
func selectPaySum(goID int, bizNum, bsDt string) *PaymentResultListType {
	statement := "select PAY_DT, PCA_CNT, PCA_AMT, REAL_PAY_AMT from cc_pay_lst where BIZ_NUM=? and BS_DT=?;"

	rows, err := cls.QueryDBbyParam(statement, bizNum, bsDt)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] selectPcaSumData: cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return nil
	}
	defer rows.Close()

	resultSum := new(PaymentResultListType)
	for rows.Next() {
		err := rows.Scan(&resultSum.PayDt, &resultSum.PcaCnt, &resultSum.PcaAmt, &resultSum.PayAmt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] selectPcaSumData: rows.Scan error(%s) \n", goID, err.Error())
			return nil
		}
	}

	return resultSum
}

// 승인/매입/입금 데이터 Insert Query 생성 & 실행
func insertMonthData(goID int, bizNum, bsDt string) int {
	delQuery := "delete from cc_aprv_sum_month where biz_num = '" + bizNum + "' and bs_dt ='" + bsDt[:6] + "';"
	insQuery := "insert into cc_aprv_sum_month (`BIZ_NUM`,`BS_DT`,`TOT_CNT`,`TOT_AMT`,`APRV_CNT`,`APRV_AMT`,`CAN_CNT`,`CAN_AMT`,`WRT_DT`) "
	insQuery += "select biz_num, left(bs_dt, 6), sum(tot_cnt), sum(tot_amt), sum(aprv_cnt), sum(aprv_amt), sum(can_cnt), sum(can_amt), '" + bsDt + "' "
	insQuery += "from cc_aprv_sum where biz_num = '" + bizNum + "' and left(bs_dt, 6) = '" + bsDt[0:6] + "' group by left(bs_dt, 6), biz_num"

	// transation begin
	tx, err := cls.DBc.Begin()
	if err != nil {
		return -1
	}

	// 오류 처리
	defer func() {
		if err != nil {
			// transaction rollback
			lprintf(1, "[ERROR][go-%d] month insert do rollback \n")
			tx.Rollback()
		}
	}()

	// transation exec
	_, err = tx.Exec(delQuery)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] del Query(%s) -> error (%s) \n", goID, delQuery, err)
		return -2
	}
	// transation exec
	_, err = tx.Exec(insQuery)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] ins Query(%s) -> error (%s) \n", goID, delQuery, err)
		return -2
	}

	// transaction commit
	err = tx.Commit()
	if err != nil {
		lprintf(1, "[ERROR][go-%d] insert month Query commit error (%s) \n", goID, err)
		return -3
	}
	return 0
}

// 승인/매입/입금 데이터 Insert Query 생성 & 실행
func insertData(goID, queryTy int, paramPtr []string, dataTy interface{}) int {

	var statement string
	var fields []string
	var inserts []string
	var params []interface{}

	fields = append(fields, "BIZ_NUM")
	inserts = append(inserts, "?")
	fields = append(fields, "BS_DT")
	inserts = append(inserts, "?")

	elements := reflect.ValueOf(dataTy).Elem()
	for k := 0; k < elements.NumField(); k++ {
		mValue := elements.Field(k)
		mType := elements.Type().Field(k)
		tag := mType.Tag
		// fmt.Printf("%10s:%10s=%10v, db: %10s\n",
		// 	mType.Name, mType.Type, mValue.Interface(), tag.Get("db")) // 이름, 타입, 값, 태그

		if len(tag.Get("db")) == 0 {
			continue
		}

		fields = append(fields, tag.Get("db"))
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, fmt.Sprint(mValue.Interface()))
	}

	switch queryTy {
	case ApprovalSum: // 승인내역 합계
		fields = append(fields, "WRT_DT")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, time.Now().Format("20060102150405"))
		fields = append(fields, "COLLECTION_STAT_DIV")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, "1")

		statement = "insert into cc_aprv_sum (" +
			strings.Join(fields, ", ") +
			") values (" + strings.Join(inserts, ", ") + ")"

	case ApprovalList: // 승인내역 합계 리스트
		fields = append(fields, "WRT_DT")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, time.Now().Format("20060102150405"))
		fields = append(fields, "COLLECTION_STAT_DIV")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, "1")

		statement = "insert into cc_aprv_lst (" +
			strings.Join(fields, ", ") +
			") values (" + strings.Join(inserts, ", ") + ")"

	case ApprovalDetail: // 승인내역 상세 리스트
		fields = append(fields, "WRT_DT")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, time.Now().Format("20060102150405"))

		weekend := "WD"
		week := time.Now().Weekday()
		if week == 0 || week == 6 {
			weekend = "HD"
		}
		fields = append(fields, "WEEK_END")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, weekend)

		statement = "insert into cc_aprv_dtl_temp (" +
			strings.Join(fields, ", ") +
			") values (" + strings.Join(inserts, ", ") + ")"

	case PurchaseSum: // 매입내역 합계
		fields = append(fields, "WRT_DT")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, time.Now().Format("20060102150405"))
		fields = append(fields, "COLLECTION_STAT_DIV")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, "1")

		statement = "insert into cc_pca_sum (" +
			strings.Join(fields, ", ") +
			") values (" + strings.Join(inserts, ", ") + ")"

	case PurchaseList: // 매입내역 합계 리스트
		fields = append(fields, "WRT_DT")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, time.Now().Format("20060102150405"))
		fields = append(fields, "COLLECTION_STAT_DIV")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, "1")

		statement = "insert into cc_pca_lst (" +
			strings.Join(fields, ", ") +
			") values (" + strings.Join(inserts, ", ") + ")"

	case PurchaseDetail: // 매입내역 상세 리스트
		fields = append(fields, "WRT_DT")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, time.Now().Format("20060102150405"))

		statement = "insert into cc_pca_dtl_temp (" +
			strings.Join(fields, ", ") +
			") values (" + strings.Join(inserts, ", ") + ")"

	case PaymentList: // 입금내역 합계 리스트
		fields = append(fields, "WRT_DT")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, time.Now().Format("20060102150405"))
		fields = append(fields, "COLLECTION_STAT_DIV")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, "1")

		statement = "insert into cc_pay_lst (" +
			strings.Join(fields, ", ") +
			") values (" + strings.Join(inserts, ", ") + ")"

	case PaymentDetail: // 입금내역 상세 리스트
		fields = append(fields, "WRT_DT")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, time.Now().Format("20060102150405"))

		statement = "insert into cc_pay_dtl_temp (" +
			strings.Join(fields, ", ") +
			") values (" + strings.Join(inserts, ", ") + ")"

	default:
		lprintf(1, "[ERROR][go-%d] unknown query type (%s) \n", goID, queryTy)
		return -1
	}

	for _, str := range paramPtr {
		params = append(params, str)
	}

	ret, err := cls.ExecDBbyParam(statement, params)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.ExecDBbyParam error(%s) \n", goID, err.Error())
		return -1
	}

	return ret
}

// 승인/매입/입금 데이터 삭제
func deleteData(goID, ty int, bizNum, bsDt string) int {
	var statememt []string

	if ty == ApprovalTy {
		statememt = append(statememt, "delete from cc_aprv_sum where BIZ_NUM=? and BS_DT=?;")
		statememt = append(statememt, "delete from cc_aprv_lst where BIZ_NUM=? and BS_DT=?;")
		statememt = append(statememt, "delete from cc_aprv_dtl where BIZ_NUM=? and BS_DT=?;")
	} else if ty == PurchaseTy {
		statememt = append(statememt, "delete from cc_pca_sum where BIZ_NUM=? and BS_DT=?;")
		statememt = append(statememt, "delete from cc_pca_lst where BIZ_NUM=? and BS_DT=?;")
		statememt = append(statememt, "delete from cc_pca_dtl where BIZ_NUM=? and BS_DT=?;")
	} else {
		statememt = append(statememt, "delete from cc_pay_lst where BIZ_NUM=? and BS_DT=?;")
		statememt = append(statememt, "delete from cc_pay_dtl where BIZ_NUM=? and BS_DT=?;")
	}

	var ret int
	var params []interface{}
	params = append(params, bizNum)
	params = append(params, bsDt)
	for _, query := range statememt {
		// lprintf(4, "[INFO] statement=%s \n", query)
		cnt, err := cls.ExecDBbyParam(query, params)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] cls.ExecDBbyParam error(%s) \n", goID, err.Error())
			return -1
		}
		ret = ret + cnt
	}

	return ret
}

// 승인/매입/입금 데이터 저장일 기준 이전 것 삭제
func deleteDataTemp(goID, ty int, bizNum, bsDt string) int {
	var statememt []string

	if ty == ApprovalTy {
		statememt = append(statememt, "delete from cc_aprv_dtl_temp where BIZ_NUM=? and BS_DT=?;")
	} else if ty == PurchaseTy {
		statememt = append(statememt, "delete from cc_pca_dtl_temp where BIZ_NUM=? and BS_DT=?;")
	} else {
		statememt = append(statememt, "delete from cc_pay_dtl_temp where BIZ_NUM=? and BS_DT=?;")
	}

	var ret int
	var params []interface{}
	params = append(params, bizNum)
	params = append(params, bsDt)

	for _, query := range statememt {
		// lprintf(4, "[INFO] statement=%s \n", query)
		cnt, err := cls.ExecDBbyParam(query, params)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] cls.ExecDBbyParam error(%s) \n", goID, err.Error())
			return -1
		}
		ret = ret + cnt
	}

	return ret
}

// 승인/매입/입금 데이터 삭제
func moveData(goID, ty int, bizNum, bsDt string) int {
	var statememt []string

	if ty == ApprovalTy {
		statememt = append(statememt, "insert into cc_aprv_dtl select * from cc_aprv_dtl_temp where BIZ_NUM=? and BS_DT=?;")
	} else if ty == PurchaseTy {
		statememt = append(statememt, "insert into cc_pca_dtl select * from cc_pca_dtl_temp where BIZ_NUM=? and BS_DT=?;")
	} else {
		statememt = append(statememt, "insert into cc_pay_dtl select * from cc_pay_dtl_temp where BIZ_NUM=? and BS_DT=?;")
	}

	var ret int
	var params []interface{}
	params = append(params, bizNum)
	params = append(params, bsDt)
	for _, query := range statememt {
		// lprintf(4, "[INFO] statement=%s \n", query)
		cnt, err := cls.ExecDBbyParam(query, params)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] cls.ExecDBbyParam error(%s) \n", goID, err.Error())
			return -1
		}
		ret = ret + cnt
	}

	return ret
}

// update approval status
// func updateApprovalSts(goID int, stsCd, bizNum, apprNo, cardNo string) int {
// 	var statememt = "update cc_aprv_dtl set STS_CD=? where BIZ_NUM=? and APRV_NO=? and CARD_NO=? and APRV_CLSS=0;"

// 	var params []interface{}
// 	params = append(params, stsCd)
// 	params = append(params, bizNum)
// 	params = append(params, apprNo)
// 	params = append(params, cardNo)

// 	// lprintf(4, "[INFO] statement=%s \n", statememt)
// 	ret, err := cls.ExecDBbyParam(statememt, params)
// 	if err != nil {
// 		lprintf(1, "[ERROR][go-%d] cls.ExecDBbyParam error(%s) \n", goID, err.Error())
// 		return -1
// 	}

// 	return ret
// }

// update approval
// func updateApprovalDtl(goID int, fields, wheres, values []string) int {
// 	for i := 0; i < len(fields); i++ {
// 		fields[i] = fields[i] + "=?"
// 	}
// 	for i := 0; i < len(wheres); i++ {
// 		wheres[i] = wheres[i] + "=?"
// 	}

// 	statememt := "update cc_aprv_dtl set " + strings.Join(fields, ", ") + " where " + strings.Join(wheres, " and ")

// 	var params []interface{}
// 	for _, value := range values {
// 		params = append(params, value)
// 	}

// 	// lprintf(4, "[INFO][go-%d] statement=%s \n", goID, statememt)
// 	ret, err := cls.ExecDBbyParam(statememt, params)
// 	if err != nil {
// 		lprintf(1, "[ERROR][go-%d] cls.ExecDBbyParam error(%s) \n", goID, err.Error())
// 		return -1
// 	}

// 	return ret
// }

// update purchase status
// func updatePurchaseSts(goID int, stsCd, bizNum, apprNo, cardNo string) int {
// 	var statememt = "update cc_pca_dtl set STS_CD=? where BIZ_NUM=? and APRV_NO=? and CARD_NO=? and APRV_CLSS=0;"

// 	var params []interface{}
// 	params = append(params, stsCd)
// 	params = append(params, bizNum)
// 	params = append(params, apprNo)
// 	params = append(params, cardNo)

// 	// lprintf(4, "[INFO] statement=%s \n", statememt)
// 	ret, err := cls.ExecDBbyParam(statememt, params)
// 	if err != nil {
// 		lprintf(1, "[ERROR][go-%d] cls.ExecDBbyParam error(%s) \n", goID, err.Error())
// 		return -1
// 	}

// 	return ret
// }

// update detail info
func updateDetail(goID, queryTy int, fields, wheres, values []string) int {
	for i := 0; i < len(fields); i++ {
		fields[i] = fields[i] + "=?"
	}
	for i := 0; i < len(wheres); i++ {
		wheres[i] = wheres[i] + "=?"
	}

	var statememt string
	if queryTy == ApprovalDetail {
		statememt = "update cc_aprv_dtl set " + strings.Join(fields, ", ") + " where " + strings.Join(wheres, " and ")
	} else if queryTy == PurchaseDetail {
		statememt = "update cc_pca_dtl set " + strings.Join(fields, ", ") + " where " + strings.Join(wheres, " and ")
	} else {
		lprintf(1, "[ERROR][go-%d] unknown query type (%s) \n", goID, queryTy)
		return -1
	}

	var params []interface{}
	for _, value := range values {
		params = append(params, value)
	}

	// lprintf(4, "[INFO][go-%d] statement=%s \n", goID, statememt)
	ret, err := cls.ExecDBbyParam(statememt, params)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.ExecDBbyParam error(%s) \n", goID, err.Error())
		return -1
	}

	return ret
}

// 매입취소건 승인테이블 실거래일자 조회(매입취소건의 거래일자는 승일일자가 전송됨)
func getRealTrDt(goID int, bizNum string, purData PurchaseDetailType) string {
	statement := "select TR_DT from cc_aprv_dtl where BIZ_NUM=? AND APRV_NO=? AND CARD_NO=? AND STS_CD=? AND APRV_AMT=?"

	rows, err := cls.QueryDBbyParam(statement, bizNum, purData.AuthNo, purData.CardNo, "3", purData.PcaAmt)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return ""
	}
	defer rows.Close()

	var orgTrDt string
	for rows.Next() {
		err := rows.Scan(&orgTrDt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] rows.Scan error(%s) \n", goID, err.Error())
			return ""
		}
	}

	return orgTrDt
}

// 승인취소건 원거래일자 조회
func getOrgTrDt(goID int, bizNum string, aprvData ApprovalDetailType) string {
	statement := "select TR_DT from cc_aprv_dtl where BIZ_NUM=? AND APRV_NO=? AND CARD_NO=? AND STS_CD=? AND APRV_AMT=?"

	rows, err := cls.QueryDBbyParam(statement, bizNum, aprvData.AuthNo, aprvData.CardNo, aprvData.StsCd, aprvData.AuthAmt)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return ""
	}
	defer rows.Close()

	var orgTrDt string
	for rows.Next() {
		err := rows.Scan(&orgTrDt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getCompInfo: rows.Scan error(%s) \n", goID, err.Error())
			return ""
		}
	}

	return orgTrDt
}

// PUSH 여부를 결정 확인
func checkPushState(goID int, bizNum, yesterDay string) bool {
	var retOk bool
	var stsCd, pushDate string

	statement := `SELECT a.STS_CD, IFNULL(b.PUSH_DT, "") FROM cc_sync_inf a, cc_comp_inf b WHERE a.BIZ_NUM=? AND a.BS_DT =? AND a.BIZ_NUM = b.BIZ_NUM`

	rows, err := cls.QueryDBbyParam(statement, bizNum, yesterDay)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] checkPushState: cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return retOk
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&stsCd, &pushDate)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] checkPushState: rows.Scan error(%s) \n", goID, err.Error())
			return retOk
		}
	}

	// 늦은 시간에는 push 하지 않는다.
	nowTime := time.Now().Format("150405")
	if nowTime >= "18" {
		lprintf(4, "[INFO][go-%d] time is late: no push(%s) \n", goID, nowTime)
		return false
	}

	// compare bs_dt
	if stsCd == "1" && pushDate != yesterDay {
		retOk = true
	}

	return retOk
}

// PUSH 전송 후 테이블을 업뎃
func updatePushState(goID int, bizNum, sendDt string) {
	query := "update cc_comp_inf set PUSH_DT = '" + sendDt + "' where biz_num = '" + bizNum + "'"

	row, err := cls.QueryDB(query)
	if err != nil {
		sendChannel("PUSH 쿼리 에러", "push save query error ["+sendDt+"]", "655403")
		cls.Lprintf(1, "[error][go-%d] %s\n", goID, err.Error())
		return
	}
	defer row.Close()
}
