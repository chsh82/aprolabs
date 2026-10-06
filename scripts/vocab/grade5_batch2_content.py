# -*- coding: utf-8 -*-
"""L3 중등 보강 2차(잔여 40건 후보 기반) - 사람이 직접 작성한 콘텐츠 데이터.

1차 배치(grade5_batch1_content.py)의 "사람이 직접 작성" 관행을 그대로
따른다 - 모든 학생용 뜻풀이·예문·오답(distractor)·오답 근거는 공식 원문
(official_meaning_short)만 보고 직접 작성했다(외부 조사·사전 추가 조회
없음). 공식 원문은 별도 필드로 그대로 보존하고 학생용 수정안으로
덮어쓰지 않는다.

오답 설계는 처음부터 1차 v2에서 검수 완료된 기준(같은 의미 분야의 실제
혼동 개념 - 부분·전체/유의어 수량차이/발음 유사/정반대 개념/대응 반대/
범위·원인 차이)을 적용한다 - 1차 v1이 썼던 "배치 내 다른 단어 뜻풀이를
무작위로 재사용"하는 방식은 처음부터 쓰지 않는다.

잔여 40건 중 2건은 콘텐츠 작성 자체를 보류했다(아래 HOLD 참고) - 전수
작성을 억지로 채우지 않는다는 지시에 따름.
"""
from __future__ import annotations

# 잔여 40건(data/import/nikl_grade5_200_full_refresh_20261007.csv 기준
# 재구성, 1차 배치 30건과 정확히 상호 배타적임을 실제 적재분과 대조해
# 재검증함) 중 콘텐츠 작성을 보류한 2건.
HOLD: dict[str, str] = {
    "G5-b5364a7010af6ca0": (  # 아멘
        "특정 종교(기독교) 기도·예배 의례에서 쓰는 용어 - 일반 교육용 "
        "어휘 문항으로 다루기에는 종교적 중립성 문제가 있어 이번에는 "
        "보류했다(1차 배치의 '벨기에'와 같은 성격의 편집 판단 - 사전에 "
        "등재된 일반 어휘이지만 교육 콘텐츠 맥락에서는 제외)."
    ),
    "G5-88ade883dcf18ae7": (  # 파키스탄
        "국가명 - 1차 배치의 '벨기에'와 동일한 사유로 보류. 의미 기준 "
        "오답(유의어/부분전체/반대개념 등)을 적용하기 어렵고, 다른 "
        "나라의 특징을 오답으로 쓰려면 검증되지 않은 사실(수도·역사 등)을 "
        "새로 끌어와야 해 사실 오류 위험이 생긴다."
    ),
}

ITEMS = [
    dict(candidate_id="G5-0c775f5c27362d4a", lemma="수시", pos="명사",
         student_definition="정해진 때가 없이 그때그때 상황에 따라 함.",
         example="선생님께서는 수업 중 수시로 학생들의 이해도를 확인하셨다."),
    dict(candidate_id="G5-2fac71523c3c7e31", lemma="숙련", pos="명사",
         student_definition="같은 일을 여러 번 되풀이해서 솜씨가 좋아짐.",
         example="그는 오랜 숙련 끝에 능숙한 목수가 되었다."),
    dict(candidate_id="G5-3aedc662710832d4", lemma="순서도", pos="명사",
         student_definition="컴퓨터로 처리할 일의 차례를 약속된 기호와 그림으로 나타낸 표.",
         example="프로그램을 만들기 전에 먼저 순서도를 그려 처리 과정을 정리했다."),
    dict(candidate_id="G5-cb0ca8ec0b1687e8", lemma="시찰", pos="명사",
         student_definition="어떤 곳을 두루 돌아다니며 실제 상황을 직접 보고 살핌.",
         example="국회의원들은 수해를 입은 지역을 시찰하기 위해 현장을 방문했다."),
    dict(candidate_id="G5-39a4e81b714f5ff4", lemma="아무개", pos="대명사",
         student_definition="누구인지 이름을 밝히지 않고 사람을 가리킬 때 쓰는 말.",
         example="그는 범인을 아무개라고만 부르며 이름을 밝히지 않았다."),
    dict(candidate_id="G5-3f37a66e5d7c35c6", lemma="아무렴", pos="감탄사",
         student_definition="상대방의 말에 '정말 그렇다'며 강하게 동의할 때 하는 말.",
         example="\"우리 내일도 같이 놀 거지?\" \"아무렴, 당연하지!\""),
    dict(candidate_id="G5-6a7b802b9c395b22", lemma="아이스박스", pos="명사",
         student_definition="얼음을 넣어 음식을 차갑게 보관하는 상자.",
         example="우리 가족은 캠핑을 갈 때 아이스박스에 음료수와 과일을 담아 갔다."),
    dict(candidate_id="G5-ebf10f49fc3e8e8c", lemma="앎", pos="명사",
         student_definition="무엇을 알고 있는 것. 또는 안다는 사실 그 자체.",
         example="스스로 모른다는 것을 깨닫는 것도 하나의 앎이다."),
    dict(candidate_id="G5-91f5a77a39d93785", lemma="연중행사", pos="명사",
         student_definition="해마다 정해진 시기에 되풀이하여 열리는 행사.",
         example="체육 대회는 우리 학교의 대표적인 연중행사이다."),
    dict(candidate_id="G5-e15695de1f4b174e", lemma="열등하다", pos="형용사",
         student_definition="보통의 수준이나 등급보다 낮다.",
         example="이 기계는 최신 기계보다 성능이 열등하다."),
    dict(candidate_id="G5-954dcec048fcf60e", lemma="외박하다", pos="동사",
         student_definition="자기 집이나 머물던 곳에서 자지 않고 다른 곳에서 밤을 지내다.",
         example="그는 친구 집에서 외박하고 다음 날 아침에 돌아왔다.", context_form="외박하고"),
    dict(candidate_id="G5-656d62d62cc25ae5", lemma="용감무쌍하다", pos="형용사",
         student_definition="비교할 데가 없을 만큼 용기가 있고 씩씩하다.",
         example="소방관은 불길 속에서도 용감무쌍하게 사람들을 구해 냈다.", context_form="용감무쌍하게"),
    dict(candidate_id="G5-6de00460b8d3f2ed", lemma="유공자", pos="명사",
         student_definition="나라나 사회를 위해 힘써 큰 공을 세운 사람.",
         example="현충일에는 나라를 위해 희생한 유공자들을 기리는 행사가 열린다."),
    dict(candidate_id="G5-62db44d358bd9745", lemma="유치장", pos="명사",
         student_definition="경찰서에서 죄를 지었다고 의심되는 사람을 잠시 가두어 두는 곳.",
         example="경찰은 사건의 용의자를 유치장에 가두고 조사를 시작했다."),
    dict(candidate_id="G5-33e172c1fb916e0d", lemma="의혹하다", pos="동사",
         student_definition="무엇이 수상하다고 느껴 의심하다.",
         example="사람들은 그의 갑작스러운 행동을 의혹하기 시작했다.", context_form="의혹하기"),
    dict(candidate_id="G5-7f73a436a64e023d", lemma="자전축", pos="명사",
         student_definition="지구와 같은 천체가 스스로 돌 때 중심이 되는 가상의 선.",
         example="지구는 자전축이 23.5도 기울어진 채로 돈다."),
    dict(candidate_id="G5-7d4953428e8c29c6", lemma="자질구레하다", pos="형용사",
         student_definition="여러 가지가 다 작고 하찮아서 별로 중요하지 않다.",
         example="서랍 속에는 자질구레한 물건들이 잔뜩 들어 있었다.", context_form="자질구레한"),
    dict(candidate_id="G5-2fd478453d644311", lemma="자책감", pos="명사",
         student_definition="자기 잘못이나 실수를 깊이 뉘우치며 스스로를 탓하는 마음.",
         example="그는 약속을 지키지 못한 것에 대해 깊은 자책감을 느꼈다."),
    dict(candidate_id="G5-41d7d493497694bb", lemma="조달", pos="명사",
         student_definition="필요한 돈이나 물건을 구하여 마련해 줌.",
         example="회사는 새로운 공장을 짓기 위한 자금 조달 방법을 고민했다."),
    dict(candidate_id="G5-9407574f98d8223a", lemma="종잡다", pos="동사",
         student_definition="대강 짐작하여 알아내다.",
         example="그의 속마음을 도무지 종잡을 수가 없었다.", context_form="종잡을"),
    dict(candidate_id="G5-298c9f8edb0f38fe", lemma="중심각", pos="명사",
         student_definition="원의 중심에서 두 반지름이 만나 이루는 각.",
         example="부채꼴의 중심각이 90도이면 그 부채꼴은 원의 4분의 1이다."),
    dict(candidate_id="G5-a1d45cd61a3f5e0b", lemma="지근지근", pos="부사",
         student_definition="머리가 자꾸 쑤시듯이 아픈 모양.",
         example="감기에 걸렸는지 머리가 지근지근 아팠다."),
    dict(candidate_id="G5-040795386d94973e", lemma="차렷하다", pos="동사",
         student_definition="몸을 똑바로 세우고 움직이지 않는 차렷 자세를 하다.",
         example="선생님의 구령에 맞춰 학생들은 일제히 차렷했다.", context_form="차렷했다"),
    dict(candidate_id="G5-5e94775bbb42107e", lemma="총합계", pos="명사",
         student_definition="모든 것을 다 더해서 셈한 수.",
         example="이번 달 용돈을 모두 더하니 총합계가 오만 원이었다."),
    dict(candidate_id="G5-ab0a37f0461f391b", lemma="축하문", pos="명사",
         student_definition="축하하는 마음을 담아 쓴 글.",
         example="졸업식에서 후배들이 쓴 축하문을 선배들에게 전달했다."),
    dict(candidate_id="G5-5a852850f4ed0e6f", lemma="춘곤증", pos="명사",
         student_definition="봄이 되면 몸이 나른하고 쉽게 피곤해지는 증상.",
         example="따뜻한 봄날 오후가 되면 춘곤증 때문에 자꾸 졸음이 쏟아졌다."),
    dict(candidate_id="G5-3a3fa28212cbd5ef", lemma="치어", pos="명사",
         student_definition="알에서 깨어난 지 얼마 안 된 어린 물고기.",
         example="양식장에서는 치어를 길러 바다로 방류하는 행사를 열었다."),
    dict(candidate_id="G5-4e23b8f2082b588a", lemma="쾌속", pos="명사",
         student_definition="속도가 매우 빠름. 또는 그런 빠른 속도.",
         example="이 여객선은 쾌속으로 운항해 목적지까지 금방 도착한다."),
    dict(candidate_id="G5-9f8a5cc594e60916", lemma="큰곰", pos="명사",
         student_definition="몸집이 매우 크고 힘이 센 곰의 한 종류로, 북반구의 산속에 사는 동물.",
         example="큰곰은 겨울이 되면 굴속에서 겨울잠을 잔다."),
    dict(candidate_id="G5-44a69a009f753aa8", lemma="타향", pos="명사",
         student_definition="자기가 태어나 자란 고향이 아닌 다른 고장.",
         example="그는 젊은 시절 타향에서 혼자 힘들게 생활했다."),
    dict(candidate_id="G5-9a8bb8abc0cfa4da", lemma="탄생석", pos="명사",
         student_definition="태어난 달을 상징하여 몸에 지니면 행운을 준다고 알려진 보석.",
         example="내 탄생석은 7월을 상징하는 루비이다."),
    dict(candidate_id="G5-849d93d9dbaa799b", lemma="턱수염", pos="명사",
         student_definition="아래턱 부분에 자라난 수염.",
         example="할아버지는 하얗게 센 턱수염을 기르고 계셨다."),
    dict(candidate_id="G5-36deb023167c53ff", lemma="포장도로", pos="명사",
         student_definition="시멘트나 아스팔트 따위로 단단하게 덮어 차와 사람이 다니기 좋게 만든 길.",
         example="공사가 끝나자 울퉁불퉁하던 흙길이 매끈한 포장도로로 바뀌었다."),
    dict(candidate_id="G5-25ba52eb835b5d79", lemma="품팔이", pos="명사",
         student_definition="삯을 받고 남의 일을 해 주는 일. 또는 그 일을 하는 사람.",
         example="옛날 가난한 농민들은 품팔이로 생계를 이어 가기도 했다."),
    dict(candidate_id="G5-80c717622f097d44", lemma="해명하다", pos="동사",
         student_definition="어떤 일의 까닭이나 속사정을 자세히 풀어서 밝히다.",
         example="그는 오해를 풀기 위해 사건의 전말을 차근차근 해명했다.", context_form="해명했다"),
    dict(candidate_id="G5-907a1f279a938291", lemma="호위하다", pos="동사",
         student_definition="곁에서 따라다니며 위험하지 않도록 보호하고 지키다.",
         example="경호원들은 대통령의 곁에서 그를 호위했다.", context_form="호위했다"),
    dict(candidate_id="G5-04c5b7256a520ffc", lemma="화전민", pos="명사",
         student_definition="산에 불을 질러 밭을 만들고 그 땅에서 농사를 짓는 사람.",
         example="산골짜기의 화전민들은 나무를 베고 불을 질러 밭을 일구었다."),
    dict(candidate_id="G5-36f40d4775d9e761", lemma="희희낙락거리다", pos="동사",
         student_definition="매우 기뻐하며 계속 즐거워하다.",
         example="아이들은 눈싸움을 하며 희희낙락거렸다.", context_form="희희낙락거렸다"),
]

assert len(ITEMS) == 38
assert len({it["candidate_id"] for it in ITEMS}) == 38
assert len({it["lemma"] for it in ITEMS}) == 38
assert set(HOLD.keys()) & {it["candidate_id"] for it in ITEMS} == set()
assert len(HOLD) == 2
