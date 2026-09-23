import json,sys
a=json.load(open('/home/claude/proto/assets.json'))
T=open('/home/claude/proto/template.html').read()
def build(data,imgs,title,brand,out):
    t=T.replace('__DATA__',open(data).read()).replace('__LOGO__',a['logo']).replace('__TITLE__',title).replace('__BRAND__',brand)
    t=t.replace('__IMGS__',json.dumps({k:a[v] for k,v in imgs.items()}))
    open(out,'w').write(t); print(out,len(t))
O='/mnt/user-data/outputs/'
build('data_ginginbam.js',{k:k for k in ['holmes','jekyll','aronnax','anne','cover','logoIvory']},'긴긴밤 10주차 · 태블릿 학습지 시안','긴긴밤 · 초등 고학년 시안',O+'ginginbam_tablet_proto.html')
build('data_yeolha.js',{'cover':'cover_yh','samjeondo':'samjeondo','logoIvory':'logoIvory','hanja5':'hanja5'},'열하일기 7주차 · 중등 태블릿 학습지 시안','열하일기 · 중학생 시안',O+'yeolha_middle_proto.html')
build('data_yaong.js',{'cover':'cover_ya','logoIvory':'logoIvory','dorothy':'dorothy','fogg':'fogg','anne':'anne','holmes':'holmes','jekyll':'jekyll','aronnax':'aronnax','ill4':'ill4','ill6':'ill6','ill7':'ill7','ill8':'ill8'},'야옹아, 가족이 되어 줄게 8주차 · 저학년 태블릿 학습지 시안','야옹아 · 초등 저학년 시안',O+'yaong_lower_proto.html')
