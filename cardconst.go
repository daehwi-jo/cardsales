package main

const siteCd = "1" //1=금결원,2=홈텍스
const datePerPage = 50
const purItemCnt = 10

const (
	CcErrNo         = "0000" // 에러없음
	CcErrDb         = "0001" // DB관련 에러
	CcErrHttp       = "0002" // HTTP 통신에러
	CcErrHttpResp   = "0003" // HTTP 응답값 에러
	CcErrParsing    = "0004" // 파싱에러
	CcErrLogin      = "0005" // 로그인 에러
	CcErrGrpId      = "0006" // 그룹아이디 조회 에러
	CcErrDataFormat = "0007" // Data Format 에러
	CcErrNoData     = "0008" // Data 없음
	CcErrExistData  = "0009" // Data 있음
	CcErrSameData   = "0010" // Data 동일
	CcErrApprCnt    = "0100" // 승인건수 에러
	CcErrApprAmt    = "0101" // 승인금액 에러
	CcErrPcaCnt     = "0102" // 매입건수 에러
	CcErrPcaAmt     = "0103" // 매입금액 에러
	CcErrPayCnt     = "0104" // 입금건수 에러
	CcErrPayAmt     = "0105" // 입금금액 에러
)

const (
	ALL   = iota // 0
	ONE          // 1 --> 지정한 하루 수집
	MON          // 2 --> 지난달 1일 부터 수집
	WEK          // 3 --> 7 일 수집
	RETRY        // 4 --> 재 수집
)

const (
	NOERR = iota
)

const (
	ApprovalTy = iota // 0
	PurchaseTy        // 1
	PaymentTy         // 2
)

// 수집리스트
const (
	ApprovalSum    = iota // 0 : 승인내역 합계
	ApprovalList          // 1 : 승인내역 합계 리스트
	ApprovalDetail        // 2 : 승인내역 상세 리스트
	PurchaseSum           // 3 : 매입내역 합계
	PurchaseList          // 4 : 매입내역 합계 리스트
	PurchaseDetail        // 5 : 매입내역 상세 리스트
	PaymentList           // 6 : 입금내역 합계 리스트
	PaymentDetail         // 7 : 입금내역 상세 리스트
)
