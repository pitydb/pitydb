package primitive

import (
	"testing"
	"unsafe"
	"fmt"
)

func TestBool(t *testing.T) {
	println(unsafe.Sizeof(Int32{}))
}

func TestInt32(t *testing.T) {
	i1 := int32(-999)
	mI1 := NewInt32()
	mI1.SetValue(i1)
	b1, _ := mI1.Marshal()

	mI2 := NewInt32()
	lenMI2, _ := mI2.Unmarshaler(b1, 0)

	println(lenMI2)
	println(mI2.GetValue().(int32))
	println(int32(uint32(i1)))
}

func TestString(t *testing.T) {
	str1 := string("太阳落山了")
	mStr1 := NewString()
	mStr1.SetValue(str1)
	b1, _ := mStr1.Marshal()
	fmt.Printf("% x \n", b1)
	mStr1.SetValue(`duang
	犀 隘 媒 媚 婿 缅 缆 缔 缕 骚 搀 搓 壹 搔 葫 募 蒋 蒂 韩 棱 椰 焚 椎 棺 榔 椭 粟 棘 酣 酥 硝 硫
	تشينغ  مسؤولية  اختيار  دان  تان  و  سحب  سحب  سحب  الفيلم الذي  اتجه إلى  أعلى  ضد  اعتقال  وتفكيك  عقد  عقد  مع  سحب  اعتراض  المحتملة  . 
	봉 놀다 링 무 푸른 책임을 현 시계 게이지 바르다 골라 뽑아 모으다 뽑다 지고 평탄한 걸어 꺾어 끌고 찍은 자 위 뜯어 밀려 저당 체포하여 퍼텐셜 안고 오물 끌고 막아 비비다
	다행히 모집 경사진 걸쳐 지출하다 고르다 들고 그 고통을 받다, 만약 무성하다 사과 모 英范 줄곧 줄기 가지 논하다 林枝 잔 카운터 분석 보드 소나무 총 구상 은결이 베고 진술하다.
	奉遊び環武青責任表現を拾ったり規則を拭いタン押吸って曲がって頂をはずして引っ張って落札者に拘勢を抱いてごみを遮る和え
	坂を上げた幸を選択を取るならその苦い茂規程（平成11年12月苗英范直と茄子の莖茅林枝杯櫃析板松銃構傑のように枕
	喪は棗になって棗を刺して棗を売ることができます
	duangduangduangduang`)
	b2, _ := mStr1.Marshal()
	fmt.Printf("% x \n", b1)

	mStr2 := NewString()
	mStr2.Unmarshaler(b2, 0)
	println(mStr2.GetValue().(string))
}
