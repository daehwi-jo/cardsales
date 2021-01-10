package main

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"charlie/cls"

	"github.com/PuerkitoBio/goquery"
	"github.com/jasonlvhit/gocron"
)

var fname string
var serID string
var lprintf func(int, string, ...interface{}) = cls.Lprintf

func main() {
	fname = cls.Cls_conf(os.Args)
	lprintf(3, "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	lprintf(3, "** start cardsales scrapping : fname(%s)\n", fname)
	lprintf(3, "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	// DB connect
	ret := cls.Db_conf(fname)
	if ret < 0 {
		lprintf(1, "[ERROR] DB connection error\n")
		return
	}
	defer cls.DBc.Close()

	// SERVER ID setting
	id, r := cls.GetTokenValue("SERVER_ID", fname)
	if r == cls.CONF_ERR {
		lprintf(1, "[ERROR] SERVER_ID not exist value\n")
		return
	}
	serID = id

	// SCHEDULER setting
	sch, r := cls.GetTokenValue("SCHEDULER", fname)
	if r == cls.CONF_ERR {
		lprintf(1, "[ERROR] SCHEDULER not exist value\n")
		return
	}
	schedules := strings.Split(sch, ",")
	g := gocron.NewScheduler()
	for _, schedule := range schedules {
		g.Every(1).Day().At(schedule).Do(collect, ALL, "", "")
	}
	g.Start()
	defer g.Clear()

	http.HandleFunc("/newMember", newMember)
	http.HandleFunc("/reCollects", reCollects)
	http.HandleFunc("/reCollect", reCollect)
	http.HandleFunc("/collect", callCollect)

	// SERVER setting
	// serIP, r := cls.GetTokenValue("SERVER_IP", fname)
	// if r == cls.CONF_ERR {
	// 	lprintf(1, "[ERROR] SERVER_IP not exist value\n")
	// 	return
	// }
	serPort, r := cls.GetTokenValue("SERVER_PORT", fname)
	if r == cls.CONF_ERR {
		lprintf(1, "[ERROR] SERVER_PORT not exist value\n")
		return
	}
	// err := http.ListenAndServe(fmt.Sprintf("localhost:%s", serPort), nil)
	// err := http.ListenAndServe(fmt.Sprintf("%s:%s", serIP, serPort), nil)
	err := http.ListenAndServe(fmt.Sprintf(":%s", serPort), nil)
	if err != nil {
		lprintf(1, "[ERROR] ListenAndServe error(%s) \n", err.Error())
		return
	}
}

// 지정가맹점의 이전월 1일부터 전날짜까지 데이터 수집
// param(필수): restId
func newMember(w http.ResponseWriter, r *http.Request) {
	// restID := r.URL.Query().Get("restId")
	restID := r.FormValue("restId")
	lprintf(3, ">> newMember START .... [%s] << \n", restID)
	if len(restID) == 0 {
		lprintf(1, "[ERROR]Required parameter missing\n")
		http.Error(w, "Required parameter missing", http.StatusBadRequest)
	} else {
		ret := collect(MON, restID, "", NEW)
		if ret <= 0 {
			// fmt.Fprintln(w, "{\"code\":\"", http.StatusBadRequest, "\",\"cnt\":\"", ret, "\"}")
			fmt.Fprintf(w, "{\"code\":\"%d\",\"cnt\":\"%d\"}\n", http.StatusBadRequest, ret)
		} else {
			// fmt.Fprintln(w, "{\"code\":\"", http.StatusOK, "\",\"cnt\":\"", ret, "\"}")
			fmt.Fprintf(w, "{\"code\":\"%d\",\"cnt\":\"%d\"}\n", http.StatusOK, ret)
		}
	}
	lprintf(3, ">> newMember END .... [%s] << \n", restID)
}

// 지정가맹점의 요청일자 기준 7일 데이터 수집
// param(필수): restId, bsDt
func reCollects(w http.ResponseWriter, r *http.Request) {
	// restID := r.URL.Query().Get("restId")
	// bsDt := r.URL.Query().Get("bsDt")
	restID := r.FormValue("restId")
	bsDt := r.FormValue("bsDt")
	lprintf(3, ">> reCollects START .... [%s:%s] << \n", restID, bsDt)
	if len(restID) == 0 || len(bsDt) == 0 {
		lprintf(1, "[ERROR]Required parameter missing\n")
		http.Error(w, "Required parameter missing", http.StatusBadRequest)
	} else {
		ret := collect(WEK, restID, bsDt, RTY)
		if ret <= 0 {
			fmt.Fprintf(w, "{\"code\":\"%d\",\"cnt\":\"%d\"}\n", http.StatusBadRequest, ret)
		} else {
			fmt.Fprintf(w, "{\"code\":\"%d\",\"cnt\":\"%d\"}\n", http.StatusOK, ret)
		}
	}
	lprintf(3, ">> reCollects END .... [%s:%s] << \n", restID, bsDt)
}

// 지정가맹점의 요청일자 1일 데이터 수집
// param(필수): restId, bsDt
func reCollect(w http.ResponseWriter, r *http.Request) {
	// restID := r.URL.Query().Get("restId")
	// bsDt := r.URL.Query().Get("bsDt")
	restID := r.FormValue("restId")
	bsDt := r.FormValue("bsDt")
	lprintf(3, ">> reCollect START .... [%s:%s] << \n", restID, bsDt)
	if len(restID) == 0 || len(bsDt) == 0 {
		lprintf(1, "[ERROR]Required parameter missing\n")
		http.Error(w, "Required parameter missing", http.StatusBadRequest)
	} else {
		ret := collect(ONE, restID, bsDt, RTY)
		if ret <= 0 {
			fmt.Fprintf(w, "{\"code\":\"%d\",\"cnt\":\"%d\"}\n", http.StatusBadRequest, ret)
		} else {
			fmt.Fprintf(w, "{\"code\":\"%d\",\"cnt\":\"%d\"}\n", http.StatusOK, ret)
		}
	}
	lprintf(3, ">> reCollect END .... [%s:%s] << \n", restID, bsDt)
}

// 기본데이터 수집
// param(선택): bsDt(없는 경우 전날자 수집)
func callCollect(w http.ResponseWriter, r *http.Request) {
	bsDt := r.FormValue("bsDt")
	lprintf(3, ">> callCollect START .... [%s] << \n", bsDt)

	ret := collect(WEK, "", bsDt, POD)
	if ret == 0 {
		fmt.Fprintf(w, "{\"code\":\"%d\",\"cnt\":\"%d\"}\n", http.StatusBadRequest, ret)
	} else {
		fmt.Fprintf(w, "{\"code\":\"%d\",\"cnt\":\"%d\"}\n", http.StatusOK, ret)
	}
	lprintf(3, ">> callCollect END ....  << \n")
}

func collect(searchTy int, restID, reqDt string, retryType int) int {
	lprintf(3, "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	lprintf(4, ">> collect START .... [%d:%s:%s] << \n", searchTy, restID, reqDt)

	// 수집일자
	today := time.Now().Format("20060102")
	var bsDt string
	if len(reqDt) > 0 {
		bsDt = reqDt
	} else {
		bsDt = time.Now().AddDate(0, 0, -1).Format("20060102")
	}
	lprintf(3, "[INFO] 오늘=%s, 조회일=%s\n", today, bsDt)

	// 날짜변경을 위해 조회기준일을 Time 값으로 변경
	timeBsDt, err := time.Parse("20060102", bsDt)
	if err != nil {
		lprintf(1, "[ERROR] time.Parse (%s) \n", err.Error())
		return -1
	}

	// 데이터를 수집할 가맹점정보 가져오기
	var compInfors []CompInfoType
	if len(restID) == 0 {
		compInfors = getCompInfos(serID, bsDt)
		if len(compInfors) == 0 {
			lprintf(4, "[INFO] getCompInfo: not found compony info \n")
			return -2
		}
	} else {
		compInfors = getCompInfosByRestID(restID, bsDt)
		if len(compInfors) == 0 {
			lprintf(4, "[INFO] getCompInfosByRestID: not found compony info (rest=%s) \n", restID)
			return -3
		}
	}
	lprintf(4, "[INFO] 가맹점정보 (%d건)(%v) \n", len(compInfors), compInfors)

	// 수집일수 체크
	var searchDay int
	if searchTy == MON { //	전달 1일 부터 수집 newMember 호출시 (최초수집)
		currentYear, currentMonth, _ := timeBsDt.Date()
		firstOfMonth := time.Date(currentYear, currentMonth, 1, 0, 0, 0, 0, timeBsDt.Location())
		firstOfMonth = firstOfMonth.AddDate(0, -1, 0)
		diff := timeBsDt.Sub(firstOfMonth)
		searchDay = int(diff.Hours()/24) + 1
		lprintf(4, "[INFO] firstOfMonth(%s)~timeBsDt(%s)=search(%d일) \n", firstOfMonth.Format("2006-01-02 15:04:05"), timeBsDt.Format("2006-01-02 15:04:05"), searchDay)
	} else if searchTy == WEK {
		searchDay = 7
	} else {
		searchDay = 1
	}

	wg := sync.WaitGroup{}
	for idx, compInfo := range compInfors {
		goID := idx
		comp := compInfo
		wg.Add(1) // WaitGroup의 고루틴 개수 1 증가
		go func() {
			defer wg.Done()

			// 수집할 날자 리스트 만듬
			var dateList []string
			startDt := timeBsDt.AddDate(0, 0, -(searchDay - 1)).Format("20060102")
			endDt := bsDt

			// 명시적 재수집이 아닌 경우 이전 데이터수집 결과 조회 (오늘 돌았던 적이 있고, 정상이면 수집 안함).
			if retryType != RTY {
				syncInfos := selectSync(goID, comp.BizNum, startDt, endDt)
				lprintf(4, "[INFO][go-%d] syncInfos=%v \n", goID, syncInfos)
				// 이전 결과 상태 체크
				if syncInfos[bsDt].StsCd != "2" && len(syncInfos[bsDt].StsCd) != 0 {
					// 오늘 수집 정상 SKIP
					lprintf(4, "[INFO][go-%d] today collect success already (%s)\n", goID, comp.BizNum)
					return
				}
			}

			// 과거 일부터 수집 시작, 따라서 오늘자 수집 데이터가 정상이면 7일치를 다 정상으로 조회했음을 의미
			for i := searchDay - 1; i >= 0; i-- {
				tmpsDt := timeBsDt.AddDate(0, 0, -(i)).Format("20060102")
				dateList = append(dateList, tmpsDt)
			}
			lprintf(4, "[INFO][go-%d] dateList (%v)\n", goID, dateList)

			// login
			resp, err := login(goID, comp.LnID, comp.LnPsw)
			if err != nil {
				lprintf(1, "[ERROR][go-%d] login fail (%s)\n", goID, err.Error())
				// Sync 결과 저장 (login 오류, 조회 시작 기준 일로)
				sync := SyncInfoType{comp.BizNum, strings.ReplaceAll(bsDt, "-", ""), siteCd, "0", "0", "0", "0", "0", "0", time.Now().Format("20060102150405"), "", "2", CcErrLogin}
				insertSync(goID, sync)
				return
			}
			cookie := resp.Cookie
			lprintf(4, "[INFO][go-%d] login succes wait 100 ms(%v) \n", goID, comp.LnID)

			// grpId가 여러개일 경우(한명이 여러 사업자를 가진 경우) 처리 추가 필요함
			grpIds, err, newCookie := getGrpId(goID, resp.Cookie, comp)
			cookie = newCookie
			if err != nil {
				lprintf(1, "[ERROR][go-%d] getGrpId (%s) \n", goID, err.Error())
				sync := SyncInfoType{comp.BizNum, strings.ReplaceAll(bsDt, "-", ""), siteCd, "0", "0", "0", "0", "0", "0", time.Now().Format("20060102150405"), "", "2", CcErrGrpId}
				insertSync(goID, sync)
				return
			}
			lprintf(4, "[INFO][go-%d] getGrpId (%v) \n", goID, grpIds)

			var result, erridx int
			if result, erridx = getSalesData(dateList, goID, comp, grpIds[0].Code, cookie); result == ERROR {
				time.Sleep(1 * time.Minute)
				result, _ = getSalesData(dateList[erridx:], goID, comp, grpIds[0].Code, cookie)

			}

			// 최종 경과가 실패가 이난 경우 push and file create
			if result != ERROR {
				// 가맹점 push
				ok := checkPushState(goID, comp.BizNum, bsDt)
				if ok {
					lprintf(4, "[INFO][go-%d] send Push:(%s) \n", goID, comp.BizNum)
					updatePushState(goID, comp.BizNum, bsDt)
					pushURI := "DaRaYo/api/common/commonPush.json?userId=" + comp.BizNum + "&userTy=5&msgCode=5002"
					cls.HttpRequest("HTTP", "GET", "api.darayo.com", "80", pushURI, true)

					// 금결원 파일생성
					// 신호를 보낼 때 마다 파일을 생성하는게 맞는지 .... 금결원에 보내기 전에 그날 변경된 내역을 전부 종합해서 보내는 것이 좋을 것 같음
					// 아래 insertsync 부분을 참고, 신규 가입자만 잘 구분해서 보내면 될 것 같음.(신규 가입 데이터는 가입 다음날 오후에 데이터가 전송되야 하므로  )
					lprintf(4, "[INFO][go-%d] make kftc file:(%s) \n", goID, comp.BizNum)
					makeURI := "CashCombine/v1/csv/makeKftcData.json?bizNum=" + comp.BizNum + "&bsDt=" + bsDt
					cls.HttpRequest("HTTP", "POST", "49.50.172.227", "7180", makeURI, true)
				}
			}

			if comp.LnFirstYn == "N" {
				updateCompInfo(goID, comp.BizNum)
				comp.LnFirstYn = "Y"
			}

		}()
	}
	wg.Wait()

	// 수집결과 체크
	sumCnt, retCnt := getResultCnt(bsDt, restID, serID)

	// 카카오워크 send -> 주기 수집인 경우
	if retryType == POD {
		if sumCnt == len(compInfors) && retCnt > 0 {
			errMsg := fmt.Sprintf("[%s]매출데이터 수집 성공/전체 (%d/%d store)", serID, sumCnt, len(compInfors))
			sendChannel("전체 가맹점 수집 성공", errMsg)
		} else {
			errMsg := fmt.Sprintf("[%s]매출데이터 수집 실패 실패 가맹점 수 (%d store)", serID, len(compInfors)-sumCnt)
			sendChannel("수집 실패 가맹점 발생", errMsg)
		}
	}
	// 신규 수집인 경우 에도 카카오 워크에 알림이 좋을 것 같음
	// if retryType == NEW {}
	lprintf(4, ">> collect END.... [%d:%s:%s][%d/%d] << \n", searchTy, restID, reqDt, sumCnt, len(compInfors))
	lprintf(3, "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	return sumCnt
}

func getSalesData(dateList []string, goID int, comp CompInfoType, code string, cookies []*http.Cookie) (int, int) {
	bizNum := comp.BizNum
	for i, selBsDt := range dateList {
		time.Sleep(5 * time.Second)
		lprintf(3, "[INFO][go-%d] 수집일=%s\n", goID, selBsDt)
		//////////////////////////////////////
		// 1.승인 내역 처리
		apprCnt, apprAmt, apprErrCd, newCookie := getApproval(goID, cookies, selBsDt, code, comp)
		if apprErrCd != CcErrNo && apprErrCd != CcErrNoData && apprErrCd != CcErrSameData {
			// Sync 결과 저장(오류)
			sync := SyncInfoType{bizNum, strings.ReplaceAll(selBsDt, "-", ""), siteCd, "0", "0", "0", "0", "0", "0", time.Now().Format("20060102150405"), "", "2", apprErrCd}
			insertSync(goID, sync)
			return ERROR, i
		}
		cookies = newCookie
		//////////////////////////////////////
		// 2.매입내역 처리
		pcaCnt, pcaAmt, pcaErrCd, newCookie := getPurchase(goID, cookies, selBsDt, code, comp)
		if pcaErrCd != CcErrNo && pcaErrCd != CcErrNoData && pcaErrCd != CcErrSameData {
			// Sync 결과 저장(오류)
			sync := SyncInfoType{bizNum, strings.ReplaceAll(selBsDt, "-", ""), siteCd, apprCnt, apprAmt, "0", "0", "0", "0", time.Now().Format("20060102150405"), "", "2", pcaErrCd}
			insertSync(goID, sync)
			return ERROR, i
		}
		cookies = newCookie
		//////////////////////////////////////
		// 3.입금내역 처리
		payCnt, payAmt, payErrCd, newCookie := getPayment(goID, cookies, selBsDt, selBsDt, code, comp)
		if payErrCd != CcErrNo && payErrCd != CcErrNoData && payErrCd != CcErrSameData {
			// Sync 결과 저장(오류)
			sync := SyncInfoType{bizNum, strings.ReplaceAll(selBsDt, "-", ""), siteCd, apprCnt, apprAmt, pcaCnt, pcaAmt, "0", "0", time.Now().Format("20060102150405"), "", "2", payErrCd}
			insertSync(goID, sync)
			return ERROR, i
		}

		//////////////////////////////////////
		// Sync 결과 저장(정상)
		lprintf(4, "[INFO][go-%d] success => %v \n", goID, selBsDt)
		sync := SyncInfoType{bizNum, strings.ReplaceAll(selBsDt, "-", ""), siteCd, apprCnt, apprAmt, pcaCnt, pcaAmt, payCnt, payAmt, time.Now().Format("20060102150405"), "", "1", CcErrNo}
		// 과거와 변경이 없는 경우 업데이트를 하지 않아서, 금결원 파일 생성을 피하게 하는 것이 좋을 것 같음
		insertSync(goID, sync)
	}
	return NOERR, 0
}

func login(goID int, loginId, password string) (*LoginResponse, error) {
	// lprintf(4, "[INFO][go-%d] loginId/password=[%s/%s]\n", goID, loginId, password)
	apiUrl := "https://www.cardsales.or.kr"
	resource := "/authentication"
	data := url.Values{}
	data.Set("j_username", loginId)
	data.Set("j_password", password)
	u, _ := url.ParseRequestURI(apiUrl)
	u.Path = resource
	urlStr := u.String()

	req, err := http.NewRequest("POST", urlStr, strings.NewReader(data.Encode()))
	if err != nil {
		lprintf(1, "[ERROR][go-%d] login: http NewRequest (%s) \n", goID, err.Error())
		return nil, err
	}

	req.Header.Set("Connection", "keep-alive")
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Origin", "https://www.cardsales.or.kr")
	req.Header.Add("Referer", "https://www.cardsales.or.kr/signin")

	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] login: http (%s) \n", goID, err)
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 302 {
		lprintf(4, "[INFO][go-%d] resp=(%s) \n", goID, resp)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] login: %s \n", goID, err.Error())
		} else {
			lprintf(1, "[ERROR][go-%d] login: http resp (%d) \n", goID, resp.StatusCode)
			err = errors.New("login http resp error")
		}
		return nil, err
	}

	cookie := resp.Cookies()
	return &LoginResponse{Cookie: cookie}, nil
}

func getGrpId(goID int, cookie []*http.Cookie, comp CompInfoType) ([]GrpIdType, error, []*http.Cookie) {
	address := "https://www.cardsales.or.kr/page/api/commonCode/merGrpCode"
	referer := "https://www.cardsales.or.kr/signin"
	respData, err, newCookie := reqHttpLoginAgain(goID, cookie, address, referer, comp)
	if err != nil {
		return nil, err, cookie
	}
	cookie = newCookie
	defer respData.Body.Close()

	var grpIds []GrpIdType
	if respData.StatusCode == http.StatusOK {
		bodyBytes, err := ioutil.ReadAll(respData.Body)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getGrpId: ioutil.ReadAll (%s) \n", goID, err.Error())
			return nil, err, cookie
		}
		// lprintf(4, "[INFO][go-%d] bodyBytes (%s) \n", goID, bodyBytes)

		if err := json.Unmarshal(bodyBytes, &grpIds); err != nil { //json byte array를 , 다른객체에 넣어줌
			lprintf(1, "[ERROR][go-%d] getGrpId: req body unmarshal (%s) \n", goID, err.Error())
			lprintf(1, "[ERROR][go-%d] getGrpId: req body=(%s) \n", goID, bodyBytes)
			return nil, err, cookie
		}

		// lprintf(4, "[INFO][go-%d] grpIds (%v) \n", goID, grpIds)
	} else {
		err = fmt.Errorf("Http resp StatusCode(%d)", respData.StatusCode)
		return nil, err, cookie
	}

	return grpIds, nil, cookie
}

// 승인내역 합계 & 리스트
func getApproval(goID int, cookie []*http.Cookie, bsDt, grpId string, comp CompInfoType) (appCnt, appAmt, errCd string, ncookie []*http.Cookie) {
	address := "https://www.cardsales.or.kr/page/api/approval/dayListAjax?stdDate=" + bsDt + "&merGrpId=" + grpId + "&cardCo=&merNo="
	referer := "https://www.cardsales.or.kr/page/approval/day"
	respData, err, newCookie := reqHttpLoginAgain(goID, cookie, address, referer, comp)
	if err != nil {
		return "", "", CcErrHttp, cookie
	}
	if respData.StatusCode != http.StatusOK {
		respData.Body.Close()
		return "", "", CcErrHttp, cookie
	}

	cookie = newCookie
	bizNum := comp.BizNum
	bodyBytes, err := ioutil.ReadAll(respData.Body)
	respData.Body.Close()
	if err != nil {
		lprintf(1, "[ERROR][go-%d] getApproval: response (%s)", goID, err)
		return "", "", CcErrHttp, cookie
	}

	var approvalSum ApprovalSumType
	if err := json.Unmarshal(bodyBytes, &approvalSum); err != nil { //json byte array를 , 다른객체에 넣어줌
		lprintf(1, "[ERROR][go-%d] getApproval: req body unmarshal (%s) \n", goID, err.Error())
		lprintf(1, "[ERROR][go-%d] getApproval: req body=(%s) \n", goID, bodyBytes)
		return "", "", CcErrParsing, cookie
	}
	lprintf(3, "[INFO][go-%d] getApproval: resp approval sum (%s:%d건)(%v) \n", goID, bizNum, len(approvalSum.ResultList), approvalSum)

	apprSum := selectApprSum(goID, bizNum, bsDt)
	if apprSum == nil {
		return "", "", CcErrDb, cookie
	}
	lprintf(4, "[INFO][go-%d] getApproval: db approval sum (%v) \n", goID, apprSum)
	if len(apprSum.TotTrnsCnt) > 0 {
		if apprSum.compare(approvalSum.ResultSum) == 0 {
			// DB의 데이터와 새로 수집한 데이터가 다른 경우, 삭제 후 재수집
			lprintf(3, "[INFO][go-%d] DB=%v\n", goID, *apprSum)
			lprintf(3, "[INFO][go-%d] WEB=%v\n", goID, approvalSum.ResultSum)

			// deleteSync(goID, bizNum, bsDt)
			deleteData(goID, ApprovalTy, bizNum, bsDt)
		} else {
			// DB의 데이터와 수집데이터가 같은 경우, 정상 응답
			lprintf(4, "[INFO][go-%d] getApproval: db approval sum amt (%v) = (%v) \n", goID, apprSum.TotTrnsAmt, approvalSum.ResultSum.TotTrnsAmt)
			return approvalSum.ResultSum.TotTrnsCnt, approvalSum.ResultSum.TotTrnsAmt, CcErrSameData, cookie
		}
	}

	tranCnt, err := strconv.Atoi(approvalSum.ResultSum.TotTrnsCnt)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] getApproval: data format (approvalSum.ResultSum.TotTrnsCnt:%s)", goID, approvalSum.ResultSum.TotTrnsCnt)
		return "", "", CcErrDataFormat, cookie
	}
	if tranCnt > 0 {
		// 승인내역 합계 DB저장
		paramStr := make([]string, 0, 5)
		paramStr = append(paramStr, bizNum)
		paramStr = append(paramStr, strings.ReplaceAll(bsDt, "-", ""))
		row := insertData(goID, ApprovalSum, paramStr, &approvalSum.ResultSum)
		if row < 0 {
			lprintf(1, "[ERROR][go-%d] getApproval: sum failed to store DB \n", goID)
			return "", "", CcErrDb, cookie
		}

		// 승인내역 합계 리스트
		var cardCoUri, amtUri, tcntUri string
		address = "https://www.cardsales.or.kr/page/api/approval/detailDayListAjax?q.mode=&q.flag=&q.merGrpId=" + grpId + "&q.cardCo=&q.merNo=&q.stdDate=" + bsDt
		for _, approvalList := range approvalSum.ResultList {
			cardCoUri = cardCoUri + "&q.cardCoArray=" + approvalList.CardCo
			amtUri = amtUri + "&amt=" + approvalList.TrnsAmt
			tcntUri = tcntUri + "&tcnt=" + approvalList.TrnsCnt

			// 승인내역 합계 리스트 DB저장
			paramStr := make([]string, 0, 5)
			paramStr = append(paramStr, bizNum)
			paramStr = append(paramStr, strings.ReplaceAll(bsDt, "-", ""))
			row := insertData(goID, ApprovalList, paramStr, &approvalList)
			if row < 0 {
				lprintf(1, "[ERROR][go-%d] getApproval: sum list failed to store DB \n", goID)
				return "", "", CcErrDb, cookie
			}
		}
		// detail
		loopCnt := tranCnt / datePerPage
		if len(approvalSum.ResultList)%datePerPage != 0 {
			loopCnt = loopCnt + 1
		}
		var updateOrgData []ApprovalDetailType
		var detailCnt, detailAmt int
		address = address + cardCoUri + amtUri + tcntUri
		for i := 1; i <= loopCnt; i++ {
			tmpAddr := address + fmt.Sprintf("&q.dataPerPage=%d&currentPage=%d", datePerPage, i)
			cnt, amt, errCd, newCookie := getApprovalDetail(goID, cookie, bsDt, tmpAddr, comp, &updateOrgData)
			if errCd != CcErrNo {
				// lprintf(1, "[ERROR][go-%d] getApproval: failed to get detail list \n", goID)
				return "", "", errCd, cookie
			}
			cookie = newCookie

			detailCnt += cnt
			detailAmt += amt
		}

		// 취소건 원거래일자 업데이트
		if len(updateOrgData) > 0 {
			for _, orgData := range updateOrgData {
				lprintf(4, "[INFO][go-%d] orgData(%v) \n", goID, orgData)
				orgData.OrgTrDt = getOrgTrDt(goID, bizNum, orgData)
				fields := []string{"ORG_TR_DT"}
				wheres := []string{"BIZ_NUM", "APRV_NO", "CARD_NO", "APRV_CLSS"}
				values := []string{orgData.OrgTrDt, bizNum, orgData.AuthNo, orgData.CardNo, "1"}
				// "update cc_aprv_dtl set ORG_TR_DT=? where BIZ_NUM=? and APRV_NO=? and CARD_NO=? and APRV_CLSS=1;"
				ret := updateDetail(goID, ApprovalDetail, fields, wheres, values)
				lprintf(3, "[INFO][go-%d] getApprovalDetail: org_tr_dt(%s) update (%d건)(%s,%s,%s) \n", goID, orgData.OrgTrDt, ret, bizNum, orgData.AuthNo, orgData.CardNo)
			}
		}

		// 합계, 상세내역 비교
		sumCnt, err := strconv.Atoi(approvalSum.ResultSum.TotTrnsCnt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getApproval: data format (approvalSum.ResultSum.TotTrnsCnt:%s)", goID, approvalSum.ResultSum.TotTrnsCnt)
			return "", "", CcErrDataFormat, cookie
		}
		sumAmt, err := strconv.Atoi(approvalSum.ResultSum.TotTrnsAmt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getApproval: data format (approvalSum.ResultSum.TotTrnsAmt:%s)", goID, approvalSum.ResultSum.TotTrnsAmt)
			return "", "", CcErrDataFormat, cookie
		}

		if sumCnt != detailCnt {
			lprintf(1, "[ERROR][go-%d] getApproval: Differ to Approval Count sum(%d):detail(%d) \n", goID, sumCnt, detailCnt)
			return "", "", CcErrApprCnt, cookie
		}

		if sumAmt != detailAmt {
			lprintf(1, "[ERROR][go-%d] getApproval: Differ to Approval Amount sum(%d):detail(%d) \n", goID, sumAmt, detailAmt)
			return "", "", CcErrApprAmt, cookie
		}

		return approvalSum.ResultSum.TotTrnsCnt, approvalSum.ResultSum.TotTrnsAmt, CcErrNo, cookie
	}

	return "0", "0", CcErrNoData, cookie
}

// 승인내역 상세 리스트
func getApprovalDetail(goID int, cookie []*http.Cookie, bsDt, address string, comp CompInfoType, orgList *[]ApprovalDetailType) (int, int, string, []*http.Cookie) {
	referer := "https://www.cardsales.or.kr/page/approval/day"
	respData, err, newCookie := reqHttpLoginAgain(goID, cookie, address, referer, comp)
	if err != nil {
		return -1, -1, CcErrHttp, cookie
	}

	if respData.StatusCode != http.StatusOK {
		respData.Body.Close()
		return -1, -1, CcErrHttp, cookie
	}

	cookie = newCookie
	bizNum := comp.BizNum

	bodyBytes, err := ioutil.ReadAll(respData.Body)
	respData.Body.Close()
	if err != nil {
		lprintf(1, "[ERROR][go-%d] getApprovalDetail: response (%s)", goID, err)
		return -1, -1, CcErrHttp, cookie
	}

	var approvalDetailList []ApprovalDetailType
	if err := json.Unmarshal(bodyBytes, &approvalDetailList); err != nil { //json byte array를 , 다른객체에 넣어줌
		lprintf(1, "[ERROR][go-%d] getApprovalDetail: req body unmarshal (%s) \n", goID, err.Error())
		lprintf(1, "[ERROR][go-%d] getApprovalDetail: req body=(%s) \n", goID, bodyBytes)
		return -1, -1, CcErrParsing, cookie
	}
	lprintf(4, "[INFO][go-%d] getApprovalDetail: resp approval detail (%s:%d건)(%v) \n", goID, bizNum, len(approvalDetailList), approvalDetailList)

	// 승인내역 상세 리스트 DB저장
	var sumAmt int
	for _, approvalDetail := range approvalDetailList {
		// 취소건일 경우 원거래의 상태를 취소로 변경
		if strings.TrimSpace(approvalDetail.AuthClss) == "1" {
			// 원거래일자 업데이트할 데이터 생성
			var orgData ApprovalDetailType
			orgData.BuzNo = bizNum
			orgData.AuthNo = approvalDetail.AuthNo
			orgData.CardNo = approvalDetail.CardNo
			orgData.StsCd = "2"
			orgData.AuthAmt = strings.Replace(approvalDetail.AuthAmt, "-", "", 1)
			lprintf(3, "[INFO][go-%d] orgData(%v) \n", goID, orgData)
			*orgList = append(*orgList, orgData)
			lprintf(3, "[INFO][go-%d] orgList(%v) \n", goID, *orgList)

			// approvalDetail.OrgTrDt = getOrgTrDt(goID, bizNum, approvalDetail)
			approvalDetail.StsCd = "3"

			fields := []string{"STS_CD"}
			wheres := []string{"BIZ_NUM", "APRV_NO", "CARD_NO", "APRV_CLSS"}
			values := []string{"2", bizNum, approvalDetail.AuthNo, approvalDetail.CardNo, "0"}
			// "update cc_aprv_dtl set STS_CD=? where BIZ_NUM=? and APRV_NO=? and CARD_NO=? and APRV_CLSS=0;"
			ret := updateDetail(goID, ApprovalDetail, fields, wheres, values)
			lprintf(3, "[INFO][go-%d] getApprovalDetail: sts_cd update (%d건)(%s,%s,%s) \n", goID, ret, bizNum, approvalDetail.AuthNo, approvalDetail.CardNo)
		} else {
			approvalDetail.StsCd = "1"
		}

		paramStr := make([]string, 0, 5)
		paramStr = append(paramStr, bizNum)
		paramStr = append(paramStr, strings.ReplaceAll(bsDt, "-", ""))
		row := insertData(goID, ApprovalDetail, paramStr, &approvalDetail)
		if row < 0 {
			lprintf(1, "[ERROR][go-%d] getApprovalDetail: detail list failed to store DB \n", goID)
			return -1, -1, CcErrDb, cookie
		}

		tmpAmt, err := strconv.Atoi(approvalDetail.AuthAmt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getApproval: data format (approvalDetail.AuthAmt:%s) \n", goID, approvalDetail.AuthAmt)
			return -1, -1, CcErrDataFormat, cookie
		}
		sumAmt += tmpAmt
	}

	sumCnt := len(approvalDetailList)

	return sumCnt, sumAmt, CcErrNo, cookie

}

// 매입내역 합계 & 리스트
func getPurchase(goID int, cookie []*http.Cookie, bsDt, grpId string, comp CompInfoType) (purCnt, purAmt, errCd string, ncookie []*http.Cookie) {
	address := "https://www.cardsales.or.kr/page/purchase/day?q.flag=&q.stdYear=&q.stdMonth=&q.pageType=&q.searchDateCode=PCA_DATE&selAmt=0&selCnt=0&oldAmt=&q.oldMerNo=&q.oldMerGrpId=&q.oldSearchDate=&q.oldCardCo=&q.merGrpId=" + grpId + "&q.cardCo=&q.merNo=&q.searchDate=" + bsDt + "&q.dataPerPage=20"
	referer := "https://www.cardsales.or.kr/page/purchase/day"
	respData, err, newCookie := reqHttpLoginAgain(goID, cookie, address, referer, comp)
	if err != nil {
		return "", "", CcErrHttp, cookie
	}

	if respData.StatusCode != http.StatusOK {
		respData.Body.Close()
		return "", "", CcErrHttpResp, cookie
	}

	bizNum := comp.BizNum
	cookie = newCookie

	doc, err := goquery.NewDocumentFromReader(respData.Body)
	respData.Body.Close()
	if err != nil {
		lprintf(1, "[ERROR][go-%d] getPurchase: goquery NewDocumentFromReader (%s) \n", goID, err.Error())
		return "", "", CcErrHttp, cookie
	}

	// parsing html
	// 합계
	var purchaseSum PurchaseSumType
	doc.Find("div.table_cell_footer").Find("tr.toptal td").Each(func(i int, s *goquery.Selection) {
		val := s.Text()
		switch i {
		case 0:
			break
		case 1: // 매입건수
			purchaseSum.ResultSum.PcaCnt = val
		case 2: // 매입합계
			purchaseSum.ResultSum.PcaScdAmt = strings.ReplaceAll(val, ",", "")
		case 3: // 가맹점 수수료
			purchaseSum.ResultSum.MerFee = strings.ReplaceAll(val, ",", "")
		case 4: // 포인트 수수료
			purchaseSum.ResultSum.PntFee = strings.ReplaceAll(val, ",", "")
		case 5: // 기타 수수료
			purchaseSum.ResultSum.EtcFee = strings.ReplaceAll(val, ",", "")
		case 6: // 수수료계
			purchaseSum.ResultSum.TotFee = strings.ReplaceAll(val, ",", "")
		case 7: // 부가세
			purchaseSum.ResultSum.VatAmt = strings.ReplaceAll(val, ",", "")
		case 8: // 지급예정합계
			purchaseSum.ResultSum.OuptExptAmt = strings.ReplaceAll(val, ",", "")
		default:
			lprintf(1, "[ERROR][go-%d] getPurchase:unknown field (%s) \n", goID, val)
			break
		}
	})
	// lprintf(3, "[INFO][go-%d] getPurchase: resp purchase sum (%s)(%v) \n", goID, bizNum, purchaseSum.ResultSum)

	pcaSum := selectPcaSum(goID, bizNum, bsDt)
	if pcaSum == nil {
		return "", "", CcErrDb, cookie
	}
	lprintf(4, "[INFO][go-%d] getPurchase: db purchase sum (%v) \n", goID, pcaSum)
	if len(pcaSum.PcaCnt) > 0 {
		if pcaSum.compare(purchaseSum.ResultSum) == 0 {
			// DB의 데이터와 수집데이터가 다른 경우, 삭제 후 재수집
			lprintf(4, "[INFO][go-%d] DB=%v\n", goID, *pcaSum)
			lprintf(4, "[INFO][go-%d] WEB=%v\n", goID, purchaseSum.ResultSum)

			// deleteSync(goID, bizNum, bsDt)
			deleteData(goID, PurchaseTy, bizNum, bsDt)
		} else {
			// DB의 데이터와 수집데이터가 같은 경우, 정상 응답
			lprintf(4, "[INFO][go-%d] getPurchase: db purchase sum same (%v) \n", goID, pcaSum)
			return purchaseSum.ResultSum.PcaCnt, purchaseSum.ResultSum.PcaScdAmt, CcErrSameData, cookie
		}
	}

	cnt, err := strconv.Atoi(purchaseSum.ResultSum.PcaCnt)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] getPurchase: data format (purchaseSum.ResultSum.PcaCnt:%s) \n", goID, purchaseSum.ResultSum.PcaCnt)
		lprintf(4, "[INFO] read data (%v)", doc)
		return "", "", CcErrDataFormat, cookie
	}
	if cnt > 0 {
		// 매입내역 합계 DB저장
		paramStr := make([]string, 0, 5)
		paramStr = append(paramStr, bizNum)
		paramStr = append(paramStr, strings.ReplaceAll(bsDt, "-", ""))
		row := insertData(goID, PurchaseSum, paramStr, &purchaseSum.ResultSum)
		if row < 0 {
			lprintf(1, "[ERROR][go-%d] getPurchase: Sum failed to store DB \n", goID)
			return "", "", CcErrDb, cookie
		}

		// 합계 상세조회
		tagBody := doc.Find("div.table_cell_body").Find("#tbodyMain")
		purchaseSum.ResultList = make([]ResultPurListType, tagBody.Find("tr#mainHistoryTr").Length())
		tagBody.Find("tr#mainHistoryTr td").Each(func(i int, s *goquery.Selection) {
			row := i / purItemCnt
			field := i % purItemCnt
			val := s.Text()
			switch field {
			case 0: // 카드사코드
				val, _ := s.Find("input").Attr("value")
				purchaseSum.ResultList[row].CardCo = val
			case 1: // 카드사명
				purchaseSum.ResultList[row].CardNm = val
			case 2: // 매입건수
				purchaseSum.ResultList[row].PcaCnt = val
			case 3: // 매입합계
				purchaseSum.ResultList[row].PcaScdAmt = strings.ReplaceAll(val, ",", "")
			case 4: // 가맹점 수수료
				purchaseSum.ResultList[row].MerFee = strings.ReplaceAll(val, ",", "")
			case 5: // 포인트 수수료
				purchaseSum.ResultList[row].PntFee = strings.ReplaceAll(val, ",", "")
			case 6: // 기타 수수료
				purchaseSum.ResultList[row].EtcFee = strings.ReplaceAll(val, ",", "")
			case 7: // 수수료계
				purchaseSum.ResultList[row].TotFee = strings.ReplaceAll(val, ",", "")
			case 8: // 부가세
				purchaseSum.ResultList[row].VatAmt = strings.ReplaceAll(val, ",", "")
			case 9: // 지급예정합계
				purchaseSum.ResultList[row].OuptExptAmt = strings.ReplaceAll(val, ",", "")
			default:
				lprintf(1, "[ERROR][go-%d] getPurchase: unknown field (%s) \n", goID, val)
				break
			}
		})
		lprintf(3, "[INFO][go-%d] getPurchase: resp purchase sum list (%s:%d건)(%v) \n", goID, bizNum, len(purchaseSum.ResultList), purchaseSum)

		var chkArrBaseUri, amtUri, tcntUri, chkArrUri string
		address = "https://www.cardsales.or.kr/page/api/purchase/dayDetail?q.flag=&q.stdYear=&q.stdMonth=&q.pageType=&q.searchDateCode=PCA_DATE&selAmt=" + purchaseSum.ResultSum.PcaScdAmt + "&selCnt=" + purchaseSum.ResultSum.PcaCnt + "&oldAmt=&q.oldMerNo=&q.oldMerGrpId=" + grpId + "&q.oldSearchDate=" + bsDt + "&q.oldCardCo=&q.merGrpId=" + grpId + "&q.cardCo=&q.merNo=&q.searchDate=" + bsDt + "&checkbox-inline=on"
		for _, purchaseList := range purchaseSum.ResultList {
			chkArrBaseUri = chkArrBaseUri + "&chkArrBase=" + purchaseList.CardCo
			amtUri = amtUri + "&amt=" + purchaseList.PcaScdAmt
			tcntUri = tcntUri + "&tcnt=" + purchaseList.PcaCnt
			chkArrUri = chkArrUri + "&q.chkArr=" + purchaseList.CardCo
			//&q.dataPerPage=20&q.chkArr=04&q.chkArr=13&q.chkArr=03&q.chkArr=21&q.chkArr=12&currentPage=1

			// 매입내역 합계 리스트 DB저장
			paramStr := make([]string, 0, 5)
			paramStr = append(paramStr, bizNum)
			paramStr = append(paramStr, strings.ReplaceAll(bsDt, "-", ""))
			row := insertData(goID, PurchaseList, paramStr, &purchaseList)
			if row < 0 {
				lprintf(1, "[ERROR][go-%d] getPurchase: sum list failed to store DB \n", goID)
				return "", "", CcErrDb, cookie
			}
		}

		// detail
		sumCnt, err := strconv.Atoi(purchaseSum.ResultSum.PcaCnt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getPurchase: data format (purchaseSum.ResultSum.PcaCnt:%s) \n", goID, purchaseSum.ResultSum.PcaCnt)
			return "", "", CcErrDataFormat, cookie
		}

		loopCnt := sumCnt / datePerPage
		if sumCnt%datePerPage != 0 {
			loopCnt = loopCnt + 1
		}

		var detailCnt, detailAmt int
		address = address + chkArrBaseUri + amtUri + tcntUri + chkArrUri
		for i := 1; i <= loopCnt; i++ {
			tmpAddr := address + fmt.Sprintf("&q.dataPerPage=%d&currentPage=%d", datePerPage, i)
			cnt, amt, errCd, newCookie := getPurchaseDetail(goID, cookie, bsDt, tmpAddr, comp)
			cookie = newCookie
			if errCd != CcErrNo {
				// lprintf(1, "[ERROR][go-%d] getPurchase: failed to get detail list from DB \n", goID)
				return "", "", errCd, cookie
			}

			detailCnt += cnt
			detailAmt += amt
		}

		// 합계, 상세내역 비교
		sumAmt, err := strconv.Atoi(purchaseSum.ResultSum.PcaScdAmt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getPurchase: data format (purchaseSum.ResultSum.PcaCnt:%s)", goID, purchaseSum.ResultSum.PcaCnt)
			return "", "", CcErrDataFormat, cookie
		}

		if sumCnt != detailCnt {
			lprintf(1, "[ERROR][go-%d] getPurchase: differ to Purchase count sum(%d):detail(%d) \n", goID, sumCnt, detailCnt)
			return "", "", CcErrPcaCnt, cookie
		} else {
			if sumAmt != detailAmt {
				lprintf(1, "[ERROR][go-%d] getPurchase: differ to Purchase amount sum(%d):detail(%d) \n", goID, sumAmt, detailAmt)
				return "", "", CcErrPcaAmt, cookie
			}
		}

		return purchaseSum.ResultSum.PcaCnt, purchaseSum.ResultSum.PcaScdAmt, CcErrNo, cookie
	}

	lprintf(3, "[INFO][go-%d] getPurchase: resp purchase sum (%s:%d건)(%v) \n", goID, bizNum, len(purchaseSum.ResultList), purchaseSum)
	return "0", "0", CcErrNoData, cookie
}

// 매입내역 상세 리스트
func getPurchaseDetail(goID int, cookie []*http.Cookie, bsDt, address string, comp CompInfoType) (purCnt, putAmt int, errCd string, ncookie []*http.Cookie) {
	referer := "https://www.cardsales.or.kr/page/purchase/day"
	respData, err, newCookie := reqHttpLoginAgain(goID, cookie, address, referer, comp)
	if err != nil {
		return -1, -1, CcErrHttp, cookie
	}

	if respData.StatusCode != http.StatusOK {
		respData.Body.Close()
		return -1, -1, CcErrHttpResp, cookie
	}
	bizNum := comp.BizNum
	cookie = newCookie

	bodyBytes, err := ioutil.ReadAll(respData.Body)
	respData.Body.Close()
	if err != nil {
		lprintf(1, "[ERROR][go-%d] getPurchaseDetail: response {%s}", goID, err)
		return -1, -1, CcErrHttp, cookie
	}

	var purchaseDetailList []PurchaseDetailType
	if err := json.Unmarshal(bodyBytes, &purchaseDetailList); err != nil { //json byte array를 , 다른객체에 넣어줌
		lprintf(1, "[ERROR][go-%d] getPurchaseDetail: req body unmarshal (%s) \n", goID, err.Error())
		lprintf(1, "[ERROR][go-%d] getPurchaseDetail: req body=(%s) \n", bodyBytes)
		return -1, -1, CcErrParsing, cookie
	}
	lprintf(4, "[INFO][go-%d] getPurchaseDetail: resp purchase detail (%s:%d건)(%v) \n", goID, bizNum, len(purchaseDetailList), purchaseDetailList)

	// 매입내역 상세 리스트 DB저장
	var pcaSum int
	for _, purchaseDetail := range purchaseDetailList {
		tmpAmt, err := strconv.Atoi(purchaseDetail.PcaAmt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getPurchaseDetail: data format (purchaseDetail.PcaAmt:%s) \n", goID, purchaseDetail.PcaAmt)
			return -1, -1, CcErrDataFormat, cookie
		}
		pcaSum = pcaSum + tmpAmt

		// 취소건일 경우 원거래의 상태를 취소로 변경
		if strings.TrimSpace(purchaseDetail.AuthClss) == "1" {
			purchaseDetail.OrgTrDt = getRealTrDt(goID, bizNum, purchaseDetail)
			if len(purchaseDetail.OrgTrDt) == 0 {
				// 승인테이블에 취소거래가 없는 경우(부정거래 의심건)
				purchaseDetail.FraudYn = "Y"
				purchaseDetail.OrgTrDt = purchaseDetail.PcaDate
			}
			purchaseDetail.StsCd = "3"

			fields := []string{"STS_CD"}
			wheres := []string{"BIZ_NUM", "APRV_NO", "CARD_NO", "APRV_CLSS"}
			values := []string{"2", bizNum, purchaseDetail.AuthNo, purchaseDetail.CardNo, "0"}
			// "update cc_pca_dtl set STS_CD=? where BIZ_NUM=? and APRV_NO=? and CARD_NO=? and APRV_CLSS=0;"
			ret := updateDetail(goID, PurchaseDetail, fields, wheres, values)
			lprintf(3, "[INFO][go-%d] getPurchaseDetail: sts_cd update (%d건)(%s,%s,%s) \n", goID, ret, bizNum, purchaseDetail.AuthNo, purchaseDetail.CardNo)
		} else {
			purchaseDetail.OrgTrDt = purchaseDetail.TrnsDate
			purchaseDetail.StsCd = "1"
		}

		paramStr := make([]string, 0, 5)
		paramStr = append(paramStr, bizNum)
		paramStr = append(paramStr, strings.ReplaceAll(bsDt, "-", ""))
		row := insertData(goID, PurchaseDetail, paramStr, &purchaseDetail)
		if row < 0 {
			lprintf(1, "[ERROR][go-%d] getPurchaseDetail: detail list failed to store DB \n", goID)
			return -1, -1, CcErrDb, cookie
		}

		// 승인상세 테이블에 매입유무 업데이트
		fields := []string{"PCA_YN"}
		wheres := []string{"BIZ_NUM", "TR_DT", "APRV_NO", "CARD_NO", "APRV_AMT"}
		values := []string{"Y", bizNum, purchaseDetail.TrnsDate, purchaseDetail.AuthNo, purchaseDetail.CardNo, purchaseDetail.PcaAmt}
		ret := updateDetail(goID, ApprovalDetail, fields, wheres, values)
		lprintf(3, "[INFO][go-%d] getPurchaseDetail: pca_yn update (%d건)(%s,%s,%s,%s,%s) \n", goID, ret, bizNum, purchaseDetail.TrnsDate, purchaseDetail.AuthNo, purchaseDetail.CardNo, purchaseDetail.PcaAmt)
	}

	return len(purchaseDetailList), pcaSum, CcErrNo, cookie

}

// 입금내역 합계 리스트
func getPayment(goID int, cookie []*http.Cookie, startDate, endDate, grpId string, comp CompInfoType) (payCnt, payAmt, errCd string, ncookie []*http.Cookie) {
	address := "https://www.cardsales.or.kr/page/api/payment/termListAjax?" + "q.startDate=" + startDate + "&q.endDate=" + endDate + "&q.merGrpId=" + grpId + "&q.cardCo=&q.merNo="
	referer := "https://www.cardsales.or.kr/page/purchase/day"
	respData, err, newCookie := reqHttpLoginAgain(goID, cookie, address, referer, comp)
	if err != nil {
		return "", "", CcErrHttp, cookie
	}
	if respData.StatusCode != http.StatusOK {
		respData.Body.Close()
		return "", "", CcErrHttpResp, cookie
	}

	bizNum := comp.BizNum
	cookie = newCookie

	bodyBytes, err := ioutil.ReadAll(respData.Body)
	respData.Body.Close()
	if err != nil {
		lprintf(1, "[ERROR][go-%d] getPayment: response (%s)", goID, err)
		return "", "", CcErrHttp, cookie
	}

	var paymentSum PaymentSumType
	if err := json.Unmarshal(bodyBytes, &paymentSum); err != nil {
		lprintf(1, "[ERROR][go-%d] getPayment: req body unmarshal (%s) \n", goID, err.Error())
		lprintf(1, "[ERROR][go-%d] getPayment: req body=(%s) \n", goID, bodyBytes)
		return "", "", CcErrParsing, cookie
	}
	lprintf(3, "[INFO][go-%d] getPayment: resp payment sum (%s:%d건)(%v) \n", goID, bizNum, len(paymentSum.ResultList), paymentSum)

	// 입금내역 합계는 기간별 조회여서 결과값이 리스트로 전송됨
	// 그러나 실제 조회는 일별로 하기때문에 리스트에 실제 데이터는 1건만 조회됨
	if len(paymentSum.ResultList) > 0 {
		// 입금내역 상세 조회
		var sumCnt, sumAmt int
		var stdDateArray, amt, tcnt string
		address := "https://www.cardsales.or.kr/page/api/payment/detailTermListAjax?" + "q.mode=&q.flag=&q.stdYear=&q.stdMonth=&q.pageType=" + "&q.merGrpId=" + grpId + "&q.cardCo=&q.merNo=" + "&q.startDate=" + startDate + "&q.endDate=" + endDate
		for _, paymentList := range paymentSum.ResultList {
			stdDateArray = stdDateArray + "&q.stdDateArray=" + paymentList.PayDt // 입금 일자
			amt = amt + "&amt=" + paymentList.PayAmt                             // 입금 합계
			tcnt = tcnt + "&tcnt=" + paymentList.PcaCnt                          // 매출 건수

			sumCnt, err = strconv.Atoi(paymentList.PcaCnt)
			if err != nil {
				lprintf(1, "[ERROR][go-%d] getPayment: data format (paymentList.PcaCnt:%s)", goID, paymentList.PcaCnt)
				return "", "", CcErrDataFormat, cookie
			}
			sumAmt, err = strconv.Atoi(paymentList.PayAmt)
			if err != nil {
				lprintf(1, "[ERROR][go-%d] getPayment: data format (paymentList.PayAmt:%s)", goID, paymentList.PayAmt)
				return "", "", CcErrDataFormat, cookie
			}

			var pay PaymentResultListType
			pay.PayDt = startDate
			pay.PcaCnt = paymentList.PcaCnt
			pay.PcaAmt = paymentList.PcaAmt
			pay.PayAmt = paymentList.PayAmt
			paySum := selectPaySum(goID, bizNum, startDate)
			if paySum == nil {
				return "", "", CcErrDb, cookie
			}

			if len(paySum.PcaCnt) > 0 {
				lprintf(4, "[INFO][go-%d] getPayment: db payment sum (%v) \n", goID, paySum)
				if paySum.compare(pay) == 0 {
					// DB의 데이터와 수집데이터가 다른 경우, 삭제 후 재수집
					lprintf(4, "[INFO][go-%d] DB=%v\n", goID, *paySum)
					lprintf(4, "[INFO][go-%d] WEB=%v\n", goID, pay)

					// deleteSync(goID, bizNum, startDate)
					deleteData(goID, PaymentTy, bizNum, startDate)
				} else {
					// DB의 데이터와 수집데이터가 같은 경우, 정상 응답
					lprintf(4, "[INFO][go-%d] getPayment: db Payment sum same (%v) \n", goID, paymentList.PayAmt)
					return paymentList.PcaCnt, paymentList.PayAmt, CcErrSameData, cookie
				}
			}

			// 입금내역 합계 리스트 DB저장
			paramStr := make([]string, 0, 5)
			paramStr = append(paramStr, bizNum)
			paramStr = append(paramStr, strings.ReplaceAll(paymentList.PayDt, "-", ""))
			row := insertData(goID, PaymentList, paramStr, &paymentList)
			if row < 0 {
				lprintf(1, "[ERROR][go-%d] getPayment: sum list failed to store DB \n", goID)
				return "", "", CcErrDb, cookie
			}
		}

		// detail
		pageNo := 1
		address = address + stdDateArray + amt + tcnt + "&q.dataPerPage=" + strconv.Itoa(datePerPage)
		detailCnt, detailAmt, errCd, newCookie := getPaymentDetail(goID, cookie, address, comp, pageNo)
		cookie = newCookie
		if errCd != CcErrNo {
			// lprintf(1, "[ERROR][go-%d] getPayment: failed to get detail list \n", goID)
			return "", "", errCd, cookie
		}

		// 합계, 상세내역 비교
		if sumCnt != detailCnt {
			lprintf(1, "[ERROR][go-%d] getPayment: Differ to Payment count sum(%d):detail(%d) \n", goID, sumCnt, detailCnt)
			return "", "", CcErrPayCnt, cookie
		} else {
			if sumAmt != detailAmt {
				lprintf(1, "[ERROR][go-%d] getPayment: Differ to Payment amount sum(%d):detail(%d) \n", goID, sumAmt, detailAmt)
				return "", "", CcErrPayAmt, cookie
			}
		}

		return strconv.Itoa(sumCnt), strconv.Itoa(sumAmt), CcErrNo, cookie
	}
	return "0", "0", CcErrNoData, cookie

}

// 입금내역 상세 리스트
func getPaymentDetail(goID int, cookie []*http.Cookie, address string, comp CompInfoType, pageNo int) (payCnt, payAmt int, errCd string, ncookie []*http.Cookie) {
	addressAndPage := address + "&currentPage=" + strconv.Itoa(pageNo)
	referer := "https://www.cardsales.or.kr/page/purchase/day"
	respData, err, newCookie := reqHttpLoginAgain(goID, cookie, addressAndPage, referer, comp)
	if err != nil {
		return -1, -1, CcErrHttp, cookie
	}

	if respData.StatusCode != http.StatusOK {
		respData.Body.Close()
		return -1, -1, CcErrHttpResp, cookie
	}

	bizNum := comp.BizNum
	cookie = newCookie

	bodyBytes, err := ioutil.ReadAll(respData.Body)
	respData.Body.Close()
	if err != nil {
		lprintf(1, "[ERROR][go-%d] getPaymentDetail:response (%s)", goID, err)
		return -1, -1, CcErrHttp, cookie
	}

	var paymentDetail []PaymentDetailType
	if err := json.Unmarshal(bodyBytes, &paymentDetail); err != nil {
		lprintf(1, "[ERROR][go-%d] getPaymentDetail: req body unmarshal (%s) \n", goID, err.Error())
		lprintf(1, "[ERROR][go-%d] getPaymentDetail: req body=(%s) \n", goID, bodyBytes)
		return -1, -1, CcErrParsing, cookie
	}
	lprintf(4, "[INFO][go-%d] getPaymentDetail: resp payment details (%s:%d건)(%v) \n", goID, bizNum, len(paymentDetail), paymentDetail)

	// 입금내역 상세 리스트 DB저장
	var detailCnt, detailAmt int
	for _, detailList := range paymentDetail {
		tmpCnt, err := strconv.Atoi(detailList.PcaCnt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getPaymentDetail: data format (detailList.PayAmt:%s) \n", goID, detailList.PayAmt)
			return -1, -1, CcErrDataFormat, cookie
		}
		tmpAmt, err := strconv.Atoi(detailList.PayAmt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getPaymentDetail: data format (detailList.PayAmt:%s) \n", goID, detailList.PayAmt)
			return -1, -1, CcErrDataFormat, cookie
		}
		detailCnt = detailCnt + tmpCnt
		detailAmt = detailAmt + tmpAmt

		paramStr := make([]string, 0, 5)
		paramStr = append(paramStr, bizNum)
		paramStr = append(paramStr, strings.ReplaceAll(detailList.PayDt, "-", ""))
		row := insertData(goID, PaymentDetail, paramStr, &detailList)
		if row < 0 {
			lprintf(1, "[ERROR][go-%d] getPaymentDetail: detail list failed to store DB \n", goID)
			return -1, -1, CcErrDb, cookie
		}
	}

	// totalCount 가 총합 건수
	// pageNo 단위 보다 totalCount 가 높을 때만 재호출
	if totalCnt, err := strconv.Atoi(paymentDetail[0].TotalCnt); err == nil {
		if (pageNo * datePerPage) < totalCnt {
			pageNo++
			cnt, amt, errCd, newCookie := getPaymentDetail(goID, cookie, address, comp, pageNo)
			cookie = newCookie
			if errCd != CcErrNo {
				// lprintf(1, "[ERROR][go-%d] getPaymentDetail: failed to get detail list \n", goID)
				return -1, -1, errCd, cookie
			}

			detailCnt += cnt
			detailAmt += amt
		}
	}
	return detailCnt, detailAmt, CcErrNo, cookie
}

func reqHttpLoginAgain(goID int, cookie []*http.Cookie, address, referer string, comp CompInfoType) (*http.Response, error, []*http.Cookie) {
	lprintf(4, "[INFO][go-%d] http NewRequest (%s) \n", goID, address)
	time.Sleep(500 * time.Millisecond)
	req, err := http.NewRequest("GET", address, nil)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] http NewRequest (%s) \n", goID, err.Error())
		return nil, err, cookie
	}
	for i := range cookie {
		req.AddCookie(cookie[i])
	}
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Host", "www.cardsales.or.kr")
	req.Header.Set("Referer", referer)

	// send request
	client := &http.Client{
		Timeout: time.Second * 10,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
	respData, err := client.Do(req)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] request (%s) \n", goID, err)
		return nil, err, cookie
	}

	if respData.StatusCode == 302 {
		lprintf(4, "[INFO][go-%d] login out=(%s) \n", goID, respData)
		respData.Body.Close()
		time.Sleep(3000 * time.Millisecond)
		resp, err := login(goID, comp.LnID, comp.LnPsw)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] login again error=(%s) \n", goID, err)
			return nil, err, cookie
		}
		cookie = resp.Cookie
		respData, err = reqHttp(goID, cookie, address, referer, comp)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] login again and request error =(%s) \n", goID, err)
			return nil, err, cookie
		}
	}

	return respData, nil, cookie
}

func reqHttp(goID int, cookie []*http.Cookie, address, referer string, comp CompInfoType) (*http.Response, error) {
	lprintf(4, "[INFO][go-%d] http NewRequest (%s) \n", goID, address)
	req, err := http.NewRequest("GET", address, nil)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] http NewRequest (%s) \n", goID, err.Error())
		return nil, err
	}
	for i := range cookie {
		req.AddCookie(cookie[i])
	}
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Host", "www.cardsales.or.kr")
	req.Header.Set("Referer", referer)

	// send request
	client := &http.Client{
		Timeout: time.Second * 10,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
	respData, err := client.Do(req)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] request (%s) \n", goID, err)
		return nil, err
	}
	return respData, nil
}

// 세션 쿠키 합치기, 새로 받은 쿠키에서 기존에 있는건 업뎃, 없으면 append
func mergeCookie(oldCookie, newCookie []*http.Cookie) []*http.Cookie {
	ocl := len(oldCookie)
	ncl := len(newCookie)

	for nci := 0; nci < ncl; nci++ {
		oci := 0
		for ; oci < ocl; oci++ {
			if oldCookie[oci] == newCookie[nci] {
				oldCookie[oci] = newCookie[nci]
				break
			}
		}
		// new cookie
		if oci == ocl {
			oldCookie = append(oldCookie, newCookie[nci])
		}
	}

	return oldCookie
}
