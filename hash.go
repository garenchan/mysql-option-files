package setaria

import "crypto/md5"

func HashToUint32(data []byte) (v uint32) {
	h := md5.New()
	h.Write(data)

	b := h.Sum(nil)

	v += uint32(b[0])
	v <<= 8
	v += uint32(b[1])
	v <<= 8
	v += uint32(b[2])
	v <<= 8
	v += uint32(b[3])

	return
}
