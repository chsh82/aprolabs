-- 혼동쌍 시드 2단계 · confusable 관계가 없던 45건 대상, 36건에 짝을 찾아 30쌍
-- (나머지 9건 - 다다익선·삼고초려·금의환향·동가홍상·가인박명·능소능대·
--  일벌백계·선풍도골·여필종부 - 은 150건 전체 중에도 진짜 헷갈릴 상대를
--  찾지 못해 묶지 않음)
-- relation(idiom_a, idiom_b, rel_type, note)
BEGIN;
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (152, 188, 'confusable', '이구동성은 여럿이 같은 말을 하는 것, 중구난방은 여럿이 제각각 떠드는 것이다.');  -- 이구동성 ↔ 중구난방
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (60, 145, 'confusable', '동문서답은 엉뚱한 대답을 하는 것, 유구무언은 아예 할 말이 없어 대답을 못 하는 것이다.');  -- 동문서답 ↔ 유구무언
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (65, 116, 'confusable', '마이동풍은 남의 말을 귀담아듣지 않고 흘려버리는 태도, 수수방관은 팔짱 끼고 지켜보며 나서지 않는 태도다.');  -- 마이동풍 ↔ 수수방관
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (84, 128, 'confusable', '반신반의는 믿음과 의심이 반반 섞인 심리 상태, 암중모색은 확실한 방법 없이 이것저것 시도해 보는 행동이다.');  -- 반신반의 ↔ 암중모색
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (101, 179, 'confusable', '사필귀정은 결국 옳고 바른 결과로 귀결된다는 뜻, 전화위복은 화가 오히려 복으로 바뀐다는 뜻이다.');  -- 사필귀정 ↔ 전화위복
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (103, 64, 'confusable', '삼십육계는 불리할 때 벗어나 도망치는 것이 상책이라는 뜻, 두문불출은 아예 집 밖으로 나가지 않고 틀어박히는 것이다.');  -- 삼십육계 ↔ 두문불출
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (108, 193, 'confusable', '선견지명은 미리 앞일을 내다보는 지혜, 천려일실은 아무리 지혜로운 사람도 생각 중에 실수가 있을 수 있다는 뜻이다.');  -- 선견지명 ↔ 천려일실
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (130, 191, 'confusable', '양자택일은 둘 중 하나를 스스로 선택하는 것, 진퇴양난은 어느 쪽으로도 가지 못하는 곤란한 처지다.');  -- 양자택일 ↔ 진퇴양난
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (141, 183, 'confusable', '온고지신은 옛것을 익혀 새것을 아는 발전적 태도, 조변석개는 원칙 없이 이랬다저랬다 자주 바꾸는 태도다.');  -- 온고지신 ↔ 조변석개
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (199, 110, 'confusable', '청천벽력은 갑자기 닥친 뜻밖의 큰 사건 자체, 설상가상은 나쁜 일이 잇따라 겹치는 것이다.');  -- 청천벽력 ↔ 설상가상
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (200, 33, 'confusable', '청출어람은 제자가 스승보다 뛰어나게 되는 것, 괄목상대는 남의 실력이 놀랍게 향상된 것을 보고 놀라는 것이다.');  -- 청출어람 ↔ 괄목상대
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (222, 200, 'confusable', '후생가외는 후배가 두려울 만큼 성장할 수 있다는 가능성, 청출어람은 제자가 실제로 스승을 능가하게 된 결과다.');  -- 후생가외 ↔ 청출어람
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (38, 97, 'confusable', '구우일모는 많은 것 가운데 극히 일부라는 뜻, 비일비재는 어떤 일이 자주 있다는 뜻으로 서로 반대된다.');  -- 구우일모 ↔ 비일비재
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (40, 88, 'confusable', '권불십년은 아무리 높은 권세도 오래가지 못한다는 뜻, 백년하청은 아무리 오래 기다려도 이루어지기 어렵다는 뜻이다.');  -- 권불십년 ↔ 백년하청
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (62, 210, 'confusable', '동분서주는 이리저리 몹시 바쁘게 돌아다니는 모습, 풍찬노숙은 객지에서 힘들게 고생하며 지내는 상황이다.');  -- 동분서주 ↔ 풍찬노숙
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (72, 219, 'confusable', '망연자실은 멍하니 넋을 잃는 것, 혼비백산은 몹시 놀라 넋이 흩어지는 것이다.');  -- 망연자실 ↔ 혼비백산
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (76, 3, 'confusable', '멸사봉공은 사사로운 이익을 버리고 공공의 일을 위해 헌신하는 것, 각고면려는 몸과 마음을 다해 부지런히 노력하는 것 자체를 가리킨다.');  -- 멸사봉공 ↔ 각고면려
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (87, 6, 'confusable', '백골난망은 은혜를 잊지 못하는 것, 각골통한은 원통함을 잊지 못하는 것이다.');  -- 백골난망 ↔ 각골통한
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (120, 61, 'confusable', '순망치한은 한쪽이 망하면 다른 쪽도 위태로워진다는 관계, 동병상련은 같은 어려움을 겪는 처지라서 서로를 가엾게 여기는 감정이다.');  -- 순망치한 ↔ 동병상련
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (122, 12, 'confusable', '시시비비는 옳고 그름 자체를 따지는 것, 갑론을박은 서로 자기 주장을 내세우며 반박하는 것이다.');  -- 시시비비 ↔ 갑론을박
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (216, 127, 'confusable', '호가호위는 남의 권세를 빌려 위세를 부리는 것, 안하무인은 자기만 믿고 남을 업신여기는 것이다.');  -- 호가호위 ↔ 안하무인
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (51, 172, 'confusable', '내우외환은 나라 안팎에서 겹치는 어려움 전반을 가리키고, 자중지란은 같은 편 안에서 일어나는 다툼만을 가리킨다.');  -- 내우외환 ↔ 자중지란
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (58, 165, 'confusable', '독야청청은 남들이 흔들려도 홀로 절개를 지키는 것, 일편단심은 한결같이 변치 않는 마음 자체를 가리킨다.');  -- 독야청청 ↔ 일편단심
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (79, 96, 'confusable', '목불인견은 눈앞의 상황이 참혹해 차마 보기 힘든 것, 비분강개는 그런 상황에 슬프고 분한 감정이 북받치는 것이다.');  -- 목불인견 ↔ 비분강개
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (123, 169, 'confusable', '식자우환은 아는 것 때문에 오히려 걱정이 생기는 것, 자격지심은 스스로 부족하다고 느끼는 마음이다.');  -- 식자우환 ↔ 자격지심
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (196, 48, 'confusable', '천우신조는 하늘이 돕는 뜻밖의 행운 자체, 기사회생은 그 덕에 죽을 위기에서 다시 살아나는 것이다.');  -- 천우신조 ↔ 기사회생
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (13, 124, 'confusable', '강구연월은 태평하고 평화로운 세상의 모습, 십장홍진은 어수선하고 번잡한 속세의 모습으로 서로 반대된다.');  -- 강구연월 ↔ 십장홍진
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (17, 205, 'confusable', '견리사의는 이익 앞에서도 의리를 먼저 생각하는 태도, 토사구팽은 필요가 없어지면 가차없이 버리는 태도로 서로 대조된다.');  -- 견리사의 ↔ 토사구팽
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (34, 146, 'confusable', '광음여류는 세월이 흐르는 속도 자체를 가리키고, 유방백세는 그렇게 오랜 세월이 지나도 이름이 남아 전해짐을 가리킨다.');  -- 광음여류 ↔ 유방백세
INSERT OR IGNORE INTO relation (idiom_a, idiom_b, rel_type, note) VALUES (69, 88, 'confusable', '만사휴의는 이미 모든 게 끝장나 손쓸 수 없게 된 상태, 백년하청은 아무리 기다려도 실현되기 어려운 일을 가리킨다.');  -- 만사휴의 ↔ 백년하청
COMMIT;
