package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/crypto/ssh"
)

const (
	currentConfigVersion = 2
	maxJSONBodyBytes     = 1 << 20
	maxConfigBytes       = 8 << 20
	maxHosts             = 512
	maxPointsPerHost     = 20000
	maxTotalPoints       = 100000
	defaultChartPoints   = 480
	maxChartPoints       = 1000
	systemInfoTTL        = 6 * time.Hour
	sshIdleTTL           = 5 * time.Minute
	maxBackoff           = 5 * time.Minute
)

// version is overridden from a release tag with -ldflags "-X main.version=...".
var version = "0.4.0"

const systemInfoCommand = "printf '__SYSINFO__\\n'\n" +
	"hostname\n" +
	"getconf _NPROCESSORS_ONLN 2>/dev/null || nproc\n" +
	"awk '/^MemTotal:/ {print $2}' /proc/meminfo\n" +
	"df -Pk / | tail -n 1 | awk '{print $2}'\n"

const metricCommand = "printf '__STAT__\\n'\n" +
	"head -n 1 /proc/stat\n" +
	"printf '__MEM__\\n'\n" +
	"awk '/^(MemTotal|MemAvailable):/ {print}' /proc/meminfo\n" +
	"printf '__NET__\\n'\n" +
	"cat /proc/net/dev\n" +
	"printf '__DISK__\\n'\n" +
	"df -P / | tail -n 1\n"

const htmlAsset = "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"
const cssAsset = "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"
const jsAsset = "H4sIAAAAAAACA8U8a3McRZKf8a9oNzq7G2ZaD2xiT9JI57XN4VtsCKS9RygUmtZMa6Zxz/TQ3SNpTkyE4c6GBexlYTEPswHeXQMXewZ2bw9Y27uOuPsnhGdkf+IvXGbWo6u6e0YDS8R9kbqrsrKy8lVZWdlTC9txYsSJm3hGxdg7ZBh1N25uhm5Unzf2jGYYJ/G8sbZeMmIvSfx2I8bmyNuKvLi54bcTL9p2g3lj9njJaPpxEka9jZbf7iYeAD4+A8MALPFbXthNAGrG6Bv9EszS9naTZxmWeWMGWxAomjfa3SDA13rkNhow35l62ubVfSThSSBKbW55SeTX4pPdKA4jji0I3TqAzhtbbhB72LLl+kE38k6G3XbCgSKvXfeiFb/RdhPomjdME5tjWJMXrQI9T29twbIJur9wqEa8OnvinzdOPnXm9LnVjWeePnNudQX4duxHMwuHeP8UvFuxF3g1YIZtVJaMeljrtrx24jzf9aLeCu9KYQRmL665He/JpBUgCmBr16PxK7C8doM1GMvLQKXtRF4ncGueNb12ZHHpqLk+3SgZNQS29swj5rx5xG11FsySuYjPQYKPS/jYoMej+PjwY38Lz0fNo/D8fDeEjv5abd2W5GyFUctNkA1IDooH1KTVIZLkm7EMotwxToH+pCDGIyDpmRnbScKnwpobeIiEr8L812b55DmzRMrVjWbnuIiMvm2AAL658EtTEFALEJdgRckAvYI/7i5RcNZNmg60WNBQ4m/urkUwjHWZhZx0O27NT3qIcLMH+kloUOP9LcM6zJsiDzShLegwDIah4W/CMAIxpg1rdmbumPHII8Zj9gJpEQ2pTu0h2FIFtfyFF4xz3damFzl+fAaMpOFFFvTawC4iNQItrLOWecQOnHrC3/Xq1qzd//vqQqptbqcT9FabHhcCPpSMjhfFYGvQkkRcR/YkrUytvDr0rpl1NzoPvDYDv9FM8MHz2+fNdcdv14Ju3YsZRiSLHlACNATXJdVWPJwOPPbuJi6YhZNwssSMC5yXnDwbjBCEvwIq7jY8B0acSbyWZaJP2XGTWrNMCMySRGDTwg+5ca9dM7a67Vrih21ggW91gGklI+xgQ4yOCrQlXTL8TaIwCLwIulAdT2yGUXJStlp2Kkrui4hsUm54sSziYYrGcRGBZcspHTEMBDs7h7qNGJOoR1QI3ODNOvCAPHF3XB80z4NlcuIZoGE4jsORlnhLjA4IfKiYi73jVApFrFEMaXoueC7yxSauE8RSXu11PBMEiCrj11zENf1cHLaBwTCnJbDzoYgeudhnKPu0IG4LYh1OeN6WdAdeAp42jkGWsL7q/c+/Gv7+pcFvf3//jzcMa2pPjsG9pBv37eoCH5hyKeXUZljvSS7JoUitZS9I2HS6E1Hk9sCW6L+Fo526l4BHR9VVXsEHdKxdFOau04obtvNcCC7C/PbONRMtTYHE9XP8YsK+UUO1NKwNWHVfUN+Mwh1SqtNRBF6bj+FUMijuATIsMCqVijE3cwxdJOxUMH3BQuWcHmIX3EYpUIPTdtHEAJFJOk00wFKyVJlMHPe/vDh858tv77yOr7+5MPzwxvCDy4NXr++/+uXwwosmJ5oNJvyMhC0fVCuQuhx4biQsg+s9o/VQ/9AhaZZxM9xZDd04ESwpGX5M1IC8yKWrJpogJHRMWebD9MyIoUcngWCAazGAKFJh3bAPxPE5YoRRZYhw8qk9MR9siWw5Jjows0+qpy1DEuuoC8q1FnmFPA0mW0DJmPsROQKVK2jrT4BAaAuKS+lGGdPW9Y9sJ9vx60kTMM0dhwip6aFzhrfjP7LT/YghcAKv3Uiacl/aMwK/zQIVw408F5+MfurdtvwoFlt2OvXazDqq+4zuBVc6bhvA5NY5W1J2+dhxE6s8a+M4idQ2yspLiq3uBbAjaFM6MfggD7Yzskixg/sQb7HtmwUzZZVI6ly3nS0/gKiShzsSdMmAeCJGr2y5JWOTkLiAYFP17b0OOL7gFJID1DCyOAtBSdj7Gi14KwhBSjrEtDFnr4MGnWmDQUCkkCLugBdJcIFcKsVrUreC3RSYY0cjngUqmOznDcvKrV1jL5AjpGRDQEXDFpQZ0H9y1SmzUImzbFpRNFCvWRxtScjj3AlwhVqD4Km3TtatMBKVDNCzZbPV0vOo1TZcjNOoDwVlHDliFC0u2wZNs+swQJPbI8acc1yjESIrBo4spNgK5wOjP0v2/pTZn9oj8kDP1UCqJJpnteYqWy3fGgxTDeKYfaFFqUZda7pRsrLdgJA98tGqRwUi4EWJ8TE5EoQFRXOTs8BAH2IfZBv+d5hm2CJgOizHSXvf0xiwGG83DHJBFZOIMY1t39v5cbhbMWeAJeBHjMePmUuLRL0KWG5Eft00dmcB0DR68O+xWXidq5gwBhrmqGF6aRF9sD7Sa3WSHsBWzNm54wAKkI+bBsKV3XatGUYVs+XX64FnLu3f/NngLxcfvPzyg2uXhm9/Mbz82eI0Ai4tTgPlS4zjkktCPYFJIijZQuGcdXdRuNIhiU6I6XnXLIUykluo2bPO7HFdcRXmk+Yix0cpLrprVHTutRXh6KEf01okAY/CYgG6KVUXCZvGQ6THhON1L/AqZrnMGmthAOdUUGqcjV76plGvmKCuuHQc0weR6HqqqemkCmF0IOTA0+yJuAMR9rMYFFbMdohEuZHvlpsgQK9dMfEcMZn6zB7PqA80TE+meMeyindswpHHH8uMhIbppak9knZf6phqsiwncNKN6tZ5r4f7cBLgEZKlCkrgZ72gDkdm5H7J6IK7x439b0zVmreFKfNRpE7sGfWIPa0RpnXVecJZmGId3f8vi3e+sbL8hSbUur8tOMGQl2uwANANOHCVWQtqCSyoD8LKQ2NozzqWFmPc3fVuml9qo9BCroDITk44eFkKWJfZMRgoZT2qEwWmwwSF0yAzER3+l3DTo4iCfy5Ck4B0cKBNON61PUOYJdFr9NcxgyDcxjyXYIVkCITPzsxwBqsh2PxYSTpKiqOPK0QaMmrV9pKdMDpPesVxqRoT7R6gLQ5HsBHtbrQ2O7EavEw8NsmNZfJ5FjFEu6nCwXNO2dQBqzggUQYku3+tdnIafyj13HYjq1zmSMvRrp3q6bO7ozX12V2pq3MT6OpZ4OaEiqr1RLvm0jeX3jK+I0mp+Rxpb8adBUNDmhDSX0ikq2OQrhYhPdCG4CC6y+0Iwqc8i40+2paATUbCJgRLhsi26HncoH9Ic2MJWsz38uOhhckjvpkjWd12otofnw8jY4BzxCtt26nyi+M5A+JvmA5heXBYZwfm9dsN7WjFssLn4ORcoXSn0w53LIzSMdVpPMrGOtnccToe7AiTv/BSpwiFpnWodcNNRGIQ454Zns+seX5gZeHKKSF23q4Z9CrGchUe6IRtdmSsDj74dP/WXeN/vgLV4lhRi9q13kYrprwy6lbfaMVVlhOiI/V8hnBFFVm8N7jz0uDrr1Enq2oDm0cd2zf2P3lz8MaVBy9fvv/523yOGma3agml6s3hzd8AkQyLyfq5JKBTjTGps7/GV0GcpxOu2pAyxW9vhSnH414McdcGNqYgqAks1ULQy45sAKx8agmkJIZrne4qblx8HHClevKZn7K1Y4MDABu1EKKw/skqHlagVxne8lph1MtiOHv6LMOgJ64tQsiGbLB0dZ9wAryCs+7H57MYT51Z+Qlo6kikOEZDyQaYo/w/cqJc84LAXOJpMs2F4d1Nuem28WzAIq+KOXzt6uDVT4dX3hzc+jk4uI/uFnllJqIymDcmMGGbn9pLL0UsVbp2X2IugEEDsDFGYnMIGrMrYJnAWN+sqAfFzIJ3nUDZWTw7ORTstimgKu4pWrjbTZrlTbfe8HAcAWPTBpyMefoPgj4MbMyfnP4XOvU+80+nzEx0lV2E1NeRtI4kVVBaiNet10Gh46IVdsE5sbF/l+/k4+z+PF9iJ4ySPtPKESIUmxgXvcfTfXll5Om/0SrBxuYWq3fyTYhlEdnLKN1xaYdC3dnsJkmYRgt+2y/zpla47ZW7HUnWva9f3f/ktmno+QwQKtifuxl4dZ6/xP1/cZohORB/PdxpKzO8lpuBdklMtRTP9NYkM+HNq5xk/87V+3/5xb2vbw8/uAWm/MGVSTAQ0/i9cYrpd68NLv8X9+lAy+1JMNW9wEtSpR688tGD934rqPnfd1IUqfyUx6k95Whogns2S9KLK+dD7NnoeFENgg68N2ORDzTapt0vQMTcsllSXbqKjrvtHEbWriAtOmEUTYguG9BIb69ORu48NxW24kQjY6yRwZXcAiCS9GuBpxlCFO6Y7LYeFYunEtihwKekBpm6XyfLmyigA8vjE2XjQVmbIO/saXyskfkPK0+fg00Cb5v9rR4DoAgUnzD+XCN2crJKhnTN/JG7Kf6GPoo/Cucm4ISDlpCxj1RibELX6Crl3Q5wxFvptlpu1LPUmJXo4+GJ58gVOqxdhq54cUIZuRWEM22HpW54FCuzDEssy8+uWRI3QFbHAK1fsahDBDiLE8fB88S8YKMSPS87vIIEdyqGCHDo+MnF/gDomafPYgeFTH5KPK7n0FcfXP/vB7/69fDaH4dXv7h36wpZGa9Z+K41CsXW830Fik3PhjvIEb/dBsNYPfuU5AclLxXzoOTlKGtVJWrrOUNGR6bWBa9HRhkTDsroKzZtwlRA7Olt4Gps2Vk+kGfHxVhw4mw3vDor04m/B2swJ15M9eHKGLrTrDmTiZqd1TPQnEKkFsskaFKuhGF02q0184xPL2YP6+tzmm5scWdi28p08uBHR8YRRUBVRzjRtSKXuV5V7uUBKL2OhxdNZw7wrJp+iAtvhQ1Opxs3aYb0Yrs/mSYoWDJKEfgtPzkZ+AB2lm1OetIMFyVSBNx/LVbyBVby9pPDqmksEAELdIwMIoh31BMgwFAqFw1/xUssVlCwFYUti9a7Z7Bx8wX1XbxEwtpQ9UEp42Gh1iMKNdOGlUcDFNk2orLVVP6a4zicvPXi+01yBDycE8kTeVtKDM8UzGDt2ylhJNae8XzX9xJxKV/CKgRPyEO0Gn3lLis1P15Fp2q11sErkIpqYTqRt+2H3fhJbvLIeLwHs8bYvq1v09oOvW7bqlHFfrvmkdori1k2MOvLJtCKAtWRrOMZt4drkDUoWGRUnYa/03Hb7cTNMFmmKTZi7/kKnD/wuX+k5e5usEtRaMtJuF/VSaQ8zYoHBs9oZRVhlkaAg4WQOAkDwlt6DQmv80vFpS0YUx76LItFyxepkWwznT/YbMXZq1FEMziq60TcuXQYFgrkGtVlSfHLNJWhFJhmJ2PtsvaJ1aTqQNQoC6pS8vLIZBcvfFLJ0j274i64TLJaW8m2aAosFqYbBF43qrbhNLxE7h/CM6MJyjKowwJetgDPM1GuAOERk007ZUEkLPpVVOMjLQ5pqzVc+gYIAXNuAaJ8St6rg5tjGxJXwfmsai+DA8CyCLGSZT19awtJiUq1jB9RZbFwSJVpNwm3tv4aTc56LKGQTqbgGbaAx2cKiXP04KJYQcDFhC3mVHUahcvn0alg9XoaMMkIQWOpKjItH16Rc6Wlbx7uArkyvd6qrOlNXbiKK5PaX8vm9oW8pfsmUvnskiEpAmVG5LM1Cp+KLrM23FFVNOwiw7LX5QDDEIeN0ZcSWMjLVMdWx2V3aGUA7NXKnUbWEDJEFoRGKoTmCCS7RHHIgfan20jG6We2DBVSrZE3MnuDUrGv34A8OtpAst8J8MJwEZiPPDKMrowsoPPRCgv1Rly0yELxx2eUOvHHSsYclnDL3jzikvG4bdsTskCbVF0lnRgoArOVkklW3CkLJ6mMm8HjsZBzBlOg+cPs4I3Xhzdv8MLbgy5Ziuo7sxEcBYD5+s5Nr+G3TwZh7bw43WJB5RkuSc4v+mhDOWXSO6uilJBW5vwkT0Isg5F6qYOWbrJz/OCrPwzff2nw4geDV76CA725oG01al2uiMN0VcjesFl54arRi9wJJhUQRIu6LBid1VQfBEVw2Jmxi8P1edIJbsj9EqOAgn3JPjB7OoE9BbuQBwdBy9z2Y3/TD/ykx+zJLBmW/pXDCN4XqTfX3wmoO4T/s4eQ2N32no7S3Eh6PkgDbpMCbtobpyMvRGhKuoDDaoZ1vOR4emUVWrBsez4bzbCPkzb8ejw/YrtVYzHhH/Ezk/44/zKBibIF6IwpKJDG7DwlavAIE1I8reZC+IluVKgA3XSWzK6AQjPu6HnhiBs1KFRnGB/lc4lECmtdZIWTHFZ/WxpFhF58jAjXCgH5YbRUjGaNTbO+jnvzWIhRCBj69YVDem5HUTE976DnJyKW5SlOwJwIAsuUORiT55BwiAxOKH+jlw2iGEQ8iYkY8UWM3ICxUU/0mI64DbKdsF0L/Np5/KKHzFPTlPLsQTjoxucALOOQ0GVOfrxww2HHayOeU74bhA1rtH7qNa2paoogZDQF/ApHpYG5D52SNGk41k5G0aHGUIcB15Yftazq/vXPBp+9z26NvrnwAU+2YV6//82FXw1+/urg4peDK5cGP/8Dq2QdvPHOt3c+rGZSe9yh5bIHzJlN7fn1flVzZadOP3V69bSJzkdxMia7t4ItjRFkQm+xe8n7q4N8Fd8Ex4hBu5MrEIaHBqRJhFqcWjcCO0xWyWodcaWoJIPSz34eemgMh6b53NW805ch7BiH+9BDCiORha9+NLh4g90mPnj3w8Er75h27tueibk3SgwqU3lWVto/XoP9GHOCdPxmV2F6yjYPKeAWFKj81o7wYAFRku7pwiLkh7K4BdQXCAFd0OFoOhyYAgbVK2Xs6JnAyY6fh6qOMjNFHnogfbJxTrcehZ0y8/ygfMLb8o+3itAq4BOuAwZiREFay9bCFBhzDPD/lLfldgPM7ig5T2Wdh5kfKeaoQs1kxED4vJ2GZKN4py3yAJxhR1uctJcRi8wc7jH5/T3jjyyncriT8K+LbFLHTVQuVfhHJaF8pHYUUBKqeQ5BwBrys74+koq4Q98nIZZ0s+TZw3EDkpA+qiHs2igZmOhhiegWaYB+5n5FDVtw4xWbrvx+fA8OHPLNQad1NqzDqYpc0RjzYtdJtSCMvXXFvHgNBWZa6KlAr2gTkHo6ZVUfntrjwMJ7Edp+1WYPFh2TM1fdJ7pJ8wksjY8tvCjHj1O16Dd+BnR/J6TEnQBgaccO7zDFLaloYOjUq+/DKRoBfN7r5eFUsCzHlVCHRxro3FRasWKOf7qIEE/AK9tYsMOhhLylnIG1Xyngt7nLqN/Ly7JaEzUb24XqEiaMQRz+2VtalbCQAvDKBB2GNypgok5BhxOtCiDWNuhA2KIAeOyL7zitdcjMLZrTo3FTspPKUvKHeFY9tH/tF8Mrn/CqnaLRT/rtggzA4OWbEJLtv/3e/n/cun/90/3f3rp391fwOrjy4eDCnW/vXBtc/93g0nt46P7i0v2PL+1fu2pQ5Negb26Ne1/f3H//34fvXhlefRkGKDOj1aLI8nPCDIOb7967+9nwl38y2clRSZZOtObhV7chOPm+a/780v5HLw7efH3/kxcfvHnj3p33792+wZcAS714Q13hd1jR4E9fDu5cGHzyGqPLFFfkOftlCpzWI2JJrDRSW61lQNPgnyhn5yT0iodT120yR0hVKABxol7ndKcRqSx/KepknipjzIBTIFzh+cgRhx6RJmJABVuG6FoocgmiM+cW9NSnNJyRyVE5NHOtkB+ZAZADld9ayQ9SOuUA+gEGCTrhDz6g+OWPRGQ5MLH0xSBFA5jAOFtgEysQV+4MQEO83Q77vQewgrEyDvz2eXWhtcgDSfNlWqbL5IdQThPoQMrpzMIMzFxgXXj+5pfHyo9ZZI1QTkI/OuB2sJLdwvE2R0NEWuKNR4D6d+mW+eDi5f0/f4aHm8/vDF6+pfDJb41dNIIwkp7wySWJ6Q7legpCAJE+ZAdBLcQUH5pTgTmLNFnAincqHn5rjkvQOoSCMTWg4zjCqidq7Yxu4mIvoqcbXrs7vPzrwZ/fGvzs8uDipw/+7VPmPu99feX+Z38Bznx75/Xhu58P3vh4//Yn+7dvwknd1E7qE/5EhyZm/TT6U8xAfqff28B8LUtZEpcO/mmNg38Tw6HTK0+lW3v99GyQ/SWIzE9cVBkrx/1Oh/ZTFsU3RjPKrpKxWxECLmTSp7rmXryBguKfd1weXv2C1/yOyajuaaUOesb5e6RCKEc9Ol4+6rc73WQNAyRWjU9bnbl+NA2dCYLSTPgwzmo0e8ltp3nTQKO2hTNLQ8yCKeLuZstPig1z9KlP27MKUjjKxyMu/XLC05vPAWeoPOp0O8FPqS3UMCTrFEBYiIqpIA6gqDFNiMomZW4/Ps2iYrm76lHyYf5ZkUxaC3hhJGtpxFGC6CPytwHJBn4UUWLBSKcZwR5lrktxQR878rOLDyBqDZrWbV4/bsiWBalVSljHFiGOKC+8IFIerD2dPtclSUl7tNJDJkERO8gQjdw+deXyaaI9G1OSNbFQ7psLH8PmNGkUlivVYh/BpzJaNvRUXYHE2GdCKZS5kLnb4T96JB2pit185sTqySfZZyxjbniQobZ0n1OZkFE6HsX4tUn4TnH3+vDFz/HOkMp/aVKZdGVxuXRN9+9eu3/99QcfXx3+53XpoMZc9+Tc0MES0NwTyybK29kCBWB3s8UaoC9WPaTQGotjfOEJ85Hr/5+32VPLAZTfFOT+ZEw4bbMartwPD6ojC6NpPlD7cUJ1UC6SpgF93XpGHRO0okJmI6KrKLaYQPfTX4GzsnH72EqPSfbtg2sbyLGNKuVQt3wWkoFdMeX7wXb3yQ4ZOdMiRc/ZrVrXsHDo/wBLqKuPAFMAAA=="

var (
	indexHTML, indexHTMLGzip = mustDecodeAsset(htmlAsset)
	styleCSS, styleCSSGzip   = mustDecodeAsset(cssAsset)
	appJS, appJSGzip         = mustDecodeAsset(jsAsset)
)

type Settings struct {
	RefreshInterval int `json:"refresh_interval"`
	HistoryMinutes  int `json:"history_minutes"`
	SSHTimeout      int `json:"ssh_timeout"`
}

func defaultSettings() Settings {
	return Settings{RefreshInterval: 15, HistoryMinutes: 60, SSHTimeout: 10}
}

func (s Settings) validate() error {
	if s.RefreshInterval < 5 || s.RefreshInterval > 3600 {
		return errors.New("采集频率必须在 5 到 3600 秒之间")
	}
	if s.HistoryMinutes < 5 || s.HistoryMinutes > 1440 {
		return errors.New("展示时长必须在 5 到 1440 分钟之间")
	}
	if s.SSHTimeout < 3 || s.SSHTimeout > 120 {
		return errors.New("SSH 超时必须在 3 到 120 秒之间")
	}
	return nil
}

type Host struct {
	ID         int     `json:"id"`
	Name       string  `json:"name"`
	Address    string  `json:"address"`
	Port       int     `json:"port"`
	Username   string  `json:"username"`
	AuthType   string  `json:"auth_type"`
	Password   *string `json:"password"`
	PrivateKey *string `json:"private_key"`
	Passphrase *string `json:"passphrase"`
	Position   int     `json:"position"`
	CreatedAt  float64 `json:"created_at"`
}

type PublicHost struct {
	ID            int    `json:"id"`
	Name          string `json:"name"`
	Address       string `json:"address"`
	Port          int    `json:"port"`
	Username      string `json:"username"`
	AuthType      string `json:"auth_type"`
	Position      int    `json:"position"`
	HasPassword   bool   `json:"has_password"`
	HasPrivateKey bool   `json:"has_private_key"`
}

type HostPatch struct {
	Name       *string `json:"name"`
	Address    *string `json:"address"`
	Port       *int    `json:"port"`
	Username   *string `json:"username"`
	AuthType   *string `json:"auth_type"`
	Password   *string `json:"password"`
	PrivateKey *string `json:"private_key"`
	Passphrase *string `json:"passphrase"`
}

type configFile struct {
	Version    int      `json:"version"`
	NextHostID int      `json:"next_host_id"`
	Settings   Settings `json:"settings"`
	Hosts      []Host   `json:"hosts"`
}

type Store struct {
	mu   sync.RWMutex
	path string
	data configFile
}

func defaultConfig() configFile {
	return configFile{Version: currentConfigVersion, NextHostID: 1, Settings: defaultSettings(), Hosts: []Host{}}
}

func cloneConfig(config configFile) configFile {
	cloned := config
	cloned.Hosts = append([]Host(nil), config.Hosts...)
	return cloned
}

func decodeConfig(raw []byte) (configFile, bool, error) {
	if len(raw) > maxConfigBytes {
		return configFile{}, false, fmt.Errorf("配置文件超过 %d 字节限制", maxConfigBytes)
	}
	var config configFile
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return configFile{}, false, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("配置文件包含多个 JSON 值")
		}
		return configFile{}, false, err
	}
	migrated := false
	if config.Version == 0 {
		config.Version = 1
		migrated = true
	}
	if config.Version < 1 || config.Version > currentConfigVersion {
		return configFile{}, false, fmt.Errorf("不支持的配置版本 %d", config.Version)
	}
	for config.Version < currentConfigVersion {
		switch config.Version {
		case 1:
			config.Version = 2
			migrated = true
		default:
			return configFile{}, false, fmt.Errorf("无法迁移配置版本 %d", config.Version)
		}
	}
	if config.Settings.RefreshInterval == 0 {
		config.Settings = defaultSettings()
		migrated = true
	}
	if err := config.Settings.validate(); err != nil {
		return configFile{}, false, err
	}
	if config.Hosts == nil {
		config.Hosts = []Host{}
		migrated = true
	}
	if len(config.Hosts) > maxHosts {
		return configFile{}, false, fmt.Errorf("主机数量超过 %d 台限制", maxHosts)
	}
	sort.SliceStable(config.Hosts, func(i, j int) bool {
		if config.Hosts[i].Position == config.Hosts[j].Position {
			return config.Hosts[i].ID < config.Hosts[j].ID
		}
		return config.Hosts[i].Position < config.Hosts[j].Position
	})
	seenIDs := make(map[int]struct{}, len(config.Hosts))
	maxID := 0
	for i := range config.Hosts {
		host := &config.Hosts[i]
		if host.ID < 1 {
			return configFile{}, false, errors.New("主机 ID 必须为正整数")
		}
		if _, exists := seenIDs[host.ID]; exists {
			return configFile{}, false, fmt.Errorf("主机 ID %d 重复", host.ID)
		}
		seenIDs[host.ID] = struct{}{}
		if host.Port == 0 {
			host.Port = 22
			migrated = true
		}
		host.Name = strings.TrimSpace(host.Name)
		host.Address = strings.TrimSpace(host.Address)
		host.Username = strings.TrimSpace(host.Username)
		if err := validateHost(*host); err != nil {
			return configFile{}, false, fmt.Errorf("主机 %d 配置无效: %w", host.ID, err)
		}
		if host.Position != i {
			host.Position = i
			migrated = true
		}
		if host.ID > maxID {
			maxID = host.ID
		}
	}
	if config.NextHostID <= maxID {
		config.NextHostID = maxID + 1
		migrated = true
	}
	if config.NextHostID < 1 {
		config.NextHostID = 1
		migrated = true
	}
	return config, migrated, nil
}

func newStore(path string) (*Store, error) {
	s := &Store{path: path}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) load() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		s.data = defaultConfig()
		return s.writePrimaryLocked()
	}
	if err != nil {
		return err
	}
	config, migrated, primaryErr := decodeConfig(raw)
	if primaryErr != nil {
		backupRaw, backupReadErr := os.ReadFile(s.path + ".bak")
		if backupReadErr != nil {
			return fmt.Errorf("读取 config.json 失败: %w；备份不可用: %v", primaryErr, backupReadErr)
		}
		backup, _, backupErr := decodeConfig(backupRaw)
		if backupErr != nil {
			return fmt.Errorf("读取 config.json 失败: %w；备份损坏: %v", primaryErr, backupErr)
		}
		s.data = backup
		log.Printf("config.json 损坏，已从 %s 恢复", s.path+".bak")
		return s.writePrimaryLocked()
	}
	s.data = config
	if migrated {
		return s.saveLocked()
	}
	return nil
}

func writeAtomic(path string, raw []byte) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	cleanup := func(writeErr error) error {
		_ = file.Close()
		_ = os.Remove(tmp)
		return writeErr
	}
	if _, err := file.Write(raw); err != nil {
		return cleanup(err)
	}
	if err := file.Sync(); err != nil {
		return cleanup(err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if directoryHandle, err := os.Open(directory); err == nil {
		// Directory fsync makes the rename durable on Linux. Some supported
		// desktop filesystems do not implement it, so failure is non-fatal.
		_ = directoryHandle.Sync()
		_ = directoryHandle.Close()
	}
	return nil
}

func (s *Store) marshalLocked() ([]byte, error) {
	raw, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return nil, err
	}
	raw = append(raw, '\n')
	return raw, nil
}

func (s *Store) writePrimaryLocked() error {
	raw, err := s.marshalLocked()
	if err != nil {
		return err
	}
	return writeAtomic(s.path, raw)
}

func (s *Store) saveLocked() error {
	if current, err := os.ReadFile(s.path); err == nil {
		if _, _, configErr := decodeConfig(current); configErr == nil {
			if err := writeAtomic(s.path+".bak", current); err != nil {
				return fmt.Errorf("写入配置备份失败: %w", err)
			}
		}
	}
	return s.writePrimaryLocked()
}

func (s *Store) exportConfig() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.marshalLocked()
}

func (s *Store) importConfig(raw []byte) error {
	config, _, err := decodeConfig(raw)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	previous := cloneConfig(s.data)
	s.data = config
	if err := s.saveLocked(); err != nil {
		s.data = previous
		return err
	}
	return nil
}

func publicHost(h Host) PublicHost {
	return PublicHost{
		ID: h.ID, Name: h.Name, Address: h.Address, Port: h.Port, Username: h.Username,
		AuthType: h.AuthType, Position: h.Position,
		HasPassword:   h.Password != nil && *h.Password != "",
		HasPrivateKey: h.PrivateKey != nil && *h.PrivateKey != "",
	}
}

func (s *Store) listHosts(private bool) ([]Host, []PublicHost) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	hosts := append([]Host(nil), s.data.Hosts...)
	sort.Slice(hosts, func(i, j int) bool {
		if hosts[i].Position == hosts[j].Position {
			return hosts[i].ID < hosts[j].ID
		}
		return hosts[i].Position < hosts[j].Position
	})
	if private {
		return hosts, nil
	}
	out := make([]PublicHost, len(hosts))
	for i, h := range hosts {
		out[i] = publicHost(h)
	}
	return nil, out
}

func (s *Store) getHost(id int) (Host, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, h := range s.data.Hosts {
		if h.ID == id {
			return h, true
		}
	}
	return Host{}, false
}

func validateHost(h Host) error {
	h.Name = strings.TrimSpace(h.Name)
	h.Address = strings.TrimSpace(h.Address)
	h.Username = strings.TrimSpace(h.Username)
	if h.Name == "" || h.Address == "" || h.Username == "" {
		return errors.New("名称、地址和用户名不能为空")
	}
	if h.Port < 1 || h.Port > 65535 {
		return errors.New("SSH 端口必须在 1 到 65535 之间")
	}
	if h.AuthType != "password" && h.AuthType != "key" {
		return errors.New("认证方式必须是 password 或 key")
	}
	if h.AuthType == "password" && (h.Password == nil || *h.Password == "") {
		return errors.New("密码登录必须填写密码")
	}
	if h.AuthType == "key" && (h.PrivateKey == nil || *h.PrivateKey == "") {
		return errors.New("密钥登录必须填写私钥")
	}
	return nil
}

func (s *Store) createHost(h Host) (PublicHost, error) {
	if h.Port == 0 {
		h.Port = 22
	}
	if err := validateHost(h); err != nil {
		return PublicHost{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.data.Hosts) >= maxHosts {
		return PublicHost{}, fmt.Errorf("主机数量已达到 %d 台限制", maxHosts)
	}
	previous := cloneConfig(s.data)
	h.Name = strings.TrimSpace(h.Name)
	h.Address = strings.TrimSpace(h.Address)
	h.Username = strings.TrimSpace(h.Username)
	h.ID = s.data.NextHostID
	s.data.NextHostID++
	h.Position = len(s.data.Hosts)
	h.CreatedAt = float64(time.Now().UnixNano()) / 1e9
	s.data.Hosts = append(s.data.Hosts, h)
	if err := s.saveLocked(); err != nil {
		s.data = previous
		return PublicHost{}, err
	}
	return publicHost(h), nil
}

func (s *Store) updateHost(id int, p HostPatch) (PublicHost, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.data.Hosts {
		h := &s.data.Hosts[i]
		if h.ID != id {
			continue
		}
		candidate := *h
		if p.Name != nil {
			candidate.Name = strings.TrimSpace(*p.Name)
		}
		if p.Address != nil {
			candidate.Address = strings.TrimSpace(*p.Address)
		}
		if p.Port != nil {
			candidate.Port = *p.Port
		}
		if p.Username != nil {
			candidate.Username = strings.TrimSpace(*p.Username)
		}
		if p.AuthType != nil {
			candidate.AuthType = *p.AuthType
		}
		if p.Password != nil {
			candidate.Password = p.Password
		}
		if p.PrivateKey != nil {
			candidate.PrivateKey = p.PrivateKey
		}
		if p.Passphrase != nil {
			candidate.Passphrase = p.Passphrase
		}
		if err := validateHost(candidate); err != nil {
			return PublicHost{}, err
		}
		previous := *h
		*h = candidate
		if err := s.saveLocked(); err != nil {
			*h = previous
			return PublicHost{}, err
		}
		return publicHost(*h), nil
	}
	return PublicHost{}, os.ErrNotExist
}

func (s *Store) deleteHost(id int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	previous := cloneConfig(s.data)
	found := false
	out := s.data.Hosts[:0]
	for _, h := range s.data.Hosts {
		if h.ID == id {
			found = true
			continue
		}
		out = append(out, h)
	}
	if !found {
		return false, nil
	}
	s.data.Hosts = out
	sort.Slice(s.data.Hosts, func(i, j int) bool { return s.data.Hosts[i].Position < s.data.Hosts[j].Position })
	for i := range s.data.Hosts {
		s.data.Hosts[i].Position = i
	}
	if err := s.saveLocked(); err != nil {
		s.data = previous
		return false, err
	}
	return true, nil
}

func (s *Store) reorder(ids []int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	previous := cloneConfig(s.data)
	if len(ids) != len(s.data.Hosts) {
		return errors.New("排序列表必须包含全部主机")
	}
	positions := make(map[int]int, len(ids))
	for i, id := range ids {
		if _, exists := positions[id]; exists {
			return errors.New("排序列表包含重复主机")
		}
		positions[id] = i
	}
	for i := range s.data.Hosts {
		pos, ok := positions[s.data.Hosts[i].ID]
		if !ok {
			return errors.New("排序列表必须包含全部主机")
		}
		s.data.Hosts[i].Position = pos
	}
	sort.Slice(s.data.Hosts, func(i, j int) bool { return s.data.Hosts[i].Position < s.data.Hosts[j].Position })
	if err := s.saveLocked(); err != nil {
		s.data = previous
		return err
	}
	return nil
}

func (s *Store) settings() Settings {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data.Settings
}

func (s *Store) updateSettings(settings Settings) error {
	if err := settings.validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	previous := s.data.Settings
	s.data.Settings = settings
	if err := s.saveLocked(); err != nil {
		s.data.Settings = previous
		return err
	}
	return nil
}

type Metric struct {
	Sequence      uint64  `json:"sequence"`
	Timestamp     float64 `json:"timestamp"`
	CPUPercent    float64 `json:"cpu_percent"`
	MemoryPercent float64 `json:"memory_percent"`
	NetworkRXMbps float64 `json:"network_rx_mbps"`
	NetworkTXMbps float64 `json:"network_tx_mbps"`
	DiskPercent   float64 `json:"disk_percent"`
}

type metricRing struct {
	points []Metric
	start  int
	size   int
}

func (r *metricRing) at(index int) Metric {
	return r.points[(r.start+index)%len(r.points)]
}

func (r *metricRing) grow(limit int) {
	capacity := len(r.points) * 2
	if capacity == 0 {
		capacity = 64
	}
	if capacity > limit {
		capacity = limit
	}
	next := make([]Metric, capacity)
	for i := 0; i < r.size; i++ {
		next[i] = r.at(i)
	}
	r.points = next
	r.start = 0
}

func (r *metricRing) append(point Metric, limit int) bool {
	if r.size < limit {
		if r.size == len(r.points) {
			r.grow(limit)
		}
		index := (r.start + r.size) % len(r.points)
		r.points[index] = point
		r.size++
		return false
	}
	r.points[r.start] = point
	r.start = (r.start + 1) % len(r.points)
	return true
}

func (r *metricRing) oldest() (Metric, bool) {
	if r.size == 0 {
		return Metric{}, false
	}
	return r.points[r.start], true
}

func (r *metricRing) popOldest() bool {
	if r.size == 0 {
		return false
	}
	r.start = (r.start + 1) % len(r.points)
	r.size--
	if r.size == 0 {
		r.start = 0
	}
	return true
}

func (r *metricRing) prune(cutoff float64) int {
	removed := 0
	for r.size > 0 {
		oldest, _ := r.oldest()
		if oldest.Timestamp >= cutoff {
			break
		}
		r.popOldest()
		removed++
	}
	return removed
}

func averageMetric(first, second Metric) Metric {
	return Metric{
		Sequence: second.Sequence, Timestamp: (first.Timestamp + second.Timestamp) / 2,
		CPUPercent:    (first.CPUPercent + second.CPUPercent) / 2,
		MemoryPercent: (first.MemoryPercent + second.MemoryPercent) / 2,
		NetworkRXMbps: (first.NetworkRXMbps + second.NetworkRXMbps) / 2,
		NetworkTXMbps: (first.NetworkTXMbps + second.NetworkTXMbps) / 2,
		DiskPercent:   (first.DiskPercent + second.DiskPercent) / 2,
	}
}

func (r *metricRing) compact() int {
	if r.size < 2 {
		return 0
	}
	before := r.size
	compacted := make([]Metric, 0, (r.size+1)/2)
	for index := 0; index < r.size; index += 2 {
		if index+1 < r.size {
			compacted = append(compacted, averageMetric(r.at(index), r.at(index+1)))
		} else {
			compacted = append(compacted, r.at(index))
		}
	}
	copy(r.points, compacted)
	r.start = 0
	r.size = len(compacted)
	return before - r.size
}

type MetricStore struct {
	mu           sync.Mutex
	data         map[int]*metricRing
	total        int
	nextSequence uint64
	perHostLimit int
	totalLimit   int
}

func newMetricStore() *MetricStore {
	return newMetricStoreWithLimits(maxPointsPerHost, maxTotalPoints)
}

func newMetricStoreWithLimits(perHostLimit, totalLimit int) *MetricStore {
	if perHostLimit < 1 {
		perHostLimit = 1
	}
	if totalLimit < 1 {
		totalLimit = 1
	}
	return &MetricStore{
		data: make(map[int]*metricRing), perHostLimit: perHostLimit, totalLimit: totalLimit,
	}
}

func (m *MetricStore) add(hostID int, metric Metric, historyMinutes int) Metric {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextSequence++
	metric.Sequence = m.nextSequence
	m.pruneHostLocked(hostID, float64(time.Now().UnixNano())/1e9-float64(historyMinutes*60))
	ring := m.data[hostID]
	if ring == nil {
		ring = &metricRing{}
		m.data[hostID] = ring
	}
	if !ring.append(metric, m.perHostLimit) {
		m.total++
	}
	for m.total > m.totalLimit {
		largestID, largestSize := 0, 0
		for id, candidate := range m.data {
			if candidate.size > largestSize {
				largestID, largestSize = id, candidate.size
			}
		}
		if largestSize > 1 {
			m.total -= m.data[largestID].compact()
			continue
		}
		oldestID, oldestTime, found := 0, math.MaxFloat64, false
		for id, candidate := range m.data {
			if oldest, ok := candidate.oldest(); ok && oldest.Timestamp < oldestTime {
				oldestID, oldestTime, found = id, oldest.Timestamp, true
			}
		}
		if !found || !m.data[oldestID].popOldest() {
			break
		}
		if found {
			m.total--
		}
		if m.data[oldestID].size == 0 {
			delete(m.data, oldestID)
		}
	}
	return metric
}

func (m *MetricStore) pruneHostLocked(hostID int, cutoff float64) {
	ring := m.data[hostID]
	if ring == nil {
		return
	}
	m.total -= ring.prune(cutoff)
	if ring.size == 0 {
		delete(m.data, hostID)
	}
}

func (m *MetricStore) selectLocked(hostID int, sinceTimestamp *float64, sinceSequence *uint64, limit int) []Metric {
	ring := m.data[hostID]
	if ring == nil {
		return []Metric{}
	}
	start := 0
	for start < ring.size {
		point := ring.at(start)
		afterTimestamp := sinceTimestamp == nil || point.Timestamp > *sinceTimestamp
		afterSequence := sinceSequence == nil || point.Sequence > *sinceSequence
		if afterTimestamp && afterSequence {
			break
		}
		start++
	}
	if start == ring.size {
		return []Metric{}
	}
	if limit < 2 {
		limit = 2
	}
	if limit > maxChartPoints {
		limit = maxChartPoints
	}
	count := ring.size - start
	if count <= limit {
		selected := make([]Metric, count)
		for index := 0; index < count; index++ {
			selected[index] = ring.at(start + index)
		}
		return selected
	}
	out := make([]Metric, 0, limit)
	last := count - 1
	lastIndex := -1
	for i := 0; i < limit; i++ {
		index := int(math.Round(float64(i*last) / float64(limit-1)))
		if index != lastIndex {
			out = append(out, ring.at(start+index))
			lastIndex = index
		}
	}
	return out
}

func (m *MetricStore) getSnapshot(hostID int, historyMinutes int, sinceTimestamp *float64, sinceSequence *uint64, limit int) ([]Metric, uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cutoff := float64(time.Now().UnixNano())/1e9 - float64(historyMinutes*60)
	m.pruneHostLocked(hostID, cutoff)
	return m.selectLocked(hostID, sinceTimestamp, sinceSequence, limit), m.nextSequence
}

func (m *MetricStore) get(hostID int, historyMinutes int, sinceTimestamp *float64, limit int) []Metric {
	metrics, _ := m.getSnapshot(hostID, historyMinutes, sinceTimestamp, nil, limit)
	return metrics
}

func (m *MetricStore) getAll(hostIDs []int, historyMinutes int, sinceTimestamp *float64, sinceSequence *uint64, limit int) (map[string][]Metric, uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cutoff := float64(time.Now().UnixNano())/1e9 - float64(historyMinutes*60)
	metrics := make(map[string][]Metric, len(hostIDs))
	for _, hostID := range hostIDs {
		m.pruneHostLocked(hostID, cutoff)
		metrics[strconv.Itoa(hostID)] = m.selectLocked(hostID, sinceTimestamp, sinceSequence, limit)
	}
	// Sequence numbers are assigned under this same lock. The returned boundary
	// therefore describes this exact snapshot and cannot skip a concurrent add.
	return metrics, m.nextSequence
}

func (m *MetricStore) remove(hostID int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ring := m.data[hostID]; ring != nil {
		m.total -= ring.size
	}
	delete(m.data, hostID)
}

func (m *MetricStore) pruneAll(historyMinutes int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cutoff := float64(time.Now().UnixNano())/1e9 - float64(historyMinutes*60)
	for id := range m.data {
		m.pruneHostLocked(id, cutoff)
	}
}

func (m *MetricStore) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[int]*metricRing)
	m.total = 0
	m.nextSequence = 0
}

func (m *MetricStore) stats() (points int, hosts int, sequence uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.total, len(m.data), m.nextSequence
}

type SystemInfo struct {
	Hostname    string `json:"hostname"`
	CPUCores    int    `json:"cpu_cores"`
	MemoryBytes int64  `json:"memory_bytes"`
	DiskBytes   int64  `json:"disk_bytes"`
}

type counters struct {
	total, idle, rx, tx uint64
	timestamp           time.Time
}

type Collector struct {
	mu             sync.Mutex
	previous       map[int]counters
	connections    map[int]*sshConnection
	maxConnections int
}

func newCollector() *Collector {
	return &Collector{
		previous: make(map[int]counters), connections: make(map[int]*sshConnection),
		maxConnections: sshConnectionLimit(),
	}
}

func sshConnectionLimit() int {
	value := 128
	if raw := strings.TrimSpace(os.Getenv("HOSTWATCH_MAX_IDLE_SSH")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 || parsed > maxHosts {
			log.Printf("HOSTWATCH_MAX_IDLE_SSH=%q 无效，使用默认值 %d", raw, value)
		} else {
			value = parsed
		}
	}
	return value
}

type sshConnection struct {
	client   *ssh.Client
	conn     net.Conn
	lastUsed time.Time
}

func (connection *sshConnection) close() {
	_ = connection.client.Close()
	_ = connection.conn.Close()
}

func collectionDeadline(ctx context.Context, timeout time.Duration) time.Time {
	deadline := time.Now().Add(timeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	return deadline
}

func (c *Collector) dial(ctx context.Context, host Host, timeout time.Duration) (*sshConnection, error) {
	auth, err := sshAuth(host)
	if err != nil {
		return nil, err
	}
	config := &ssh.ClientConfig{
		User: host.Username, Auth: []ssh.AuthMethod{auth},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	address := net.JoinHostPort(host.Address, strconv.Itoa(host.Port))
	dialer := net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	rawConn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("SSH 连接失败: %w", err)
	}
	if err := rawConn.SetDeadline(collectionDeadline(ctx, timeout)); err != nil {
		_ = rawConn.Close()
		return nil, fmt.Errorf("设置 SSH deadline 失败: %w", err)
	}
	stopCancellation := context.AfterFunc(ctx, func() { _ = rawConn.Close() })
	defer stopCancellation()
	clientConn, channels, requests, err := ssh.NewClientConn(rawConn, address, config)
	if err != nil {
		_ = rawConn.Close()
		return nil, fmt.Errorf("SSH 握手失败: %w", err)
	}
	client := ssh.NewClient(clientConn, channels, requests)
	connection := &sshConnection{client: client, conn: rawConn, lastUsed: time.Now()}
	if err := rawConn.SetDeadline(time.Time{}); err != nil {
		connection.close()
		return nil, fmt.Errorf("清除 SSH deadline 失败: %w", err)
	}
	return connection, nil
}

func (c *Collector) connection(ctx context.Context, host Host, timeout time.Duration) (*sshConnection, error) {
	c.mu.Lock()
	connection := c.connections[host.ID]
	if connection != nil && time.Since(connection.lastUsed) <= sshIdleTTL {
		c.mu.Unlock()
		return connection, nil
	}
	if connection != nil {
		delete(c.connections, host.ID)
	}
	c.mu.Unlock()
	if connection != nil {
		connection.close()
	}

	connection, err := c.dial(ctx, host, timeout)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		connection.close()
		return nil, err
	}
	c.mu.Lock()
	previous := c.connections[host.ID]
	if previous != nil || len(c.connections) < c.maxConnections {
		c.connections[host.ID] = connection
	}
	c.mu.Unlock()
	if previous != nil {
		previous.close()
	}
	return connection, nil
}

func (c *Collector) discardConnection(hostID int, connection *sshConnection) {
	c.mu.Lock()
	if c.connections[hostID] == connection {
		delete(c.connections, hostID)
	}
	c.mu.Unlock()
	connection.close()
}

func (c *Collector) runCommand(ctx context.Context, host Host, timeout time.Duration, command string) ([]byte, error) {
	connection, err := c.connection(ctx, host, timeout)
	if err != nil {
		return nil, err
	}
	if err := connection.conn.SetDeadline(collectionDeadline(ctx, timeout)); err != nil {
		c.discardConnection(host.ID, connection)
		return nil, err
	}
	stopCancellation := context.AfterFunc(ctx, func() {
		// SetDeadline interrupts an active SSH read/write without leaving a
		// permanently blocked goroutine behind.
		_ = connection.conn.SetDeadline(time.Now())
	})
	defer stopCancellation()

	session, err := connection.client.NewSession()
	if err != nil {
		c.discardConnection(host.ID, connection)
		// Idle SSH connections can be closed by the remote side. Reconnect once.
		connection, err = c.connection(ctx, host, timeout)
		if err != nil {
			return nil, fmt.Errorf("重连 SSH 失败: %w", err)
		}
		if err = connection.conn.SetDeadline(collectionDeadline(ctx, timeout)); err != nil {
			c.discardConnection(host.ID, connection)
			return nil, err
		}
		session, err = connection.client.NewSession()
	}
	if err != nil {
		c.discardConnection(host.ID, connection)
		return nil, fmt.Errorf("创建 SSH 会话失败: %w", err)
	}
	defer session.Close()
	output, err := session.CombinedOutput(command)
	if err != nil {
		c.discardConnection(host.ID, connection)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, fmt.Errorf("SSH 采集超时或已取消: %w", ctxErr)
		}
		return nil, fmt.Errorf("远端采集失败: %w", err)
	}
	if err := connection.conn.SetDeadline(time.Time{}); err != nil {
		c.discardConnection(host.ID, connection)
		return nil, err
	}
	c.mu.Lock()
	pooled := false
	if c.connections[host.ID] == connection {
		connection.lastUsed = time.Now()
		pooled = true
	}
	c.mu.Unlock()
	if !pooled {
		connection.close()
	}
	return output, nil
}

func (c *Collector) collect(ctx context.Context, host Host, timeout time.Duration, includeInfo bool) (Metric, *SystemInfo, error) {
	command := metricCommand
	if includeInfo {
		command = systemInfoCommand + metricCommand
	}
	output, err := c.runCommand(ctx, host, timeout, command)
	if err != nil {
		return Metric{}, nil, err
	}
	metric, err := c.parseMetric(host.ID, string(output))
	if err != nil {
		return Metric{}, nil, err
	}
	var info *SystemInfo
	if includeInfo {
		parsed, parseErr := parseSystemInfo(string(output))
		if parseErr != nil {
			return Metric{}, nil, parseErr
		}
		info = &parsed
	}
	return metric, info, nil
}

func sshAuth(host Host) (ssh.AuthMethod, error) {
	if host.AuthType == "password" {
		if host.Password == nil {
			return nil, errors.New("缺少 SSH 密码")
		}
		return ssh.Password(*host.Password), nil
	}
	if host.PrivateKey == nil {
		return nil, errors.New("缺少 SSH 私钥")
	}
	var signer ssh.Signer
	var err error
	if host.Passphrase != nil && *host.Passphrase != "" {
		signer, err = ssh.ParsePrivateKeyWithPassphrase([]byte(*host.PrivateKey), []byte(*host.Passphrase))
	} else {
		signer, err = ssh.ParsePrivateKey([]byte(*host.PrivateKey))
	}
	if err != nil {
		return nil, fmt.Errorf("私钥格式或口令无效: %w", err)
	}
	return ssh.PublicKeys(signer), nil
}

func splitSections(output string) map[string][]string {
	sections := make(map[string][]string)
	current := ""
	for _, raw := range strings.Split(output, "\n") {
		line := strings.TrimSpace(raw)
		if strings.HasPrefix(line, "__") && strings.HasSuffix(line, "__") {
			current = strings.Trim(line, "_")
			sections[current] = []string{}
		} else if current != "" && line != "" {
			sections[current] = append(sections[current], raw)
		}
	}
	return sections
}

func parseUint(value string) (uint64, error) {
	return strconv.ParseUint(strings.TrimSpace(value), 10, 64)
}

func (c *Collector) parseMetric(hostID int, output string) (Metric, error) {
	sections := splitSections(output)
	stat := sections["STAT"]
	memLines := sections["MEM"]
	netLines := sections["NET"]
	diskLines := sections["DISK"]
	if len(stat) == 0 || len(memLines) == 0 || len(netLines) == 0 || len(diskLines) == 0 {
		return Metric{}, errors.New("无法解析远端资源数据")
	}
	cpuFields := strings.Fields(stat[0])
	if len(cpuFields) < 5 {
		return Metric{}, errors.New("无法解析 CPU 数据")
	}
	var total uint64
	var values []uint64
	for _, field := range cpuFields[1:] {
		value, err := parseUint(field)
		if err != nil {
			return Metric{}, err
		}
		values = append(values, value)
		total += value
	}
	idle := values[3]
	if len(values) > 4 {
		idle += values[4]
	}

	var memTotal, memAvailable uint64
	for _, line := range memLines {
		fields := strings.Fields(strings.ReplaceAll(line, ":", ""))
		if len(fields) < 2 {
			continue
		}
		value, err := parseUint(fields[1])
		if err != nil {
			return Metric{}, err
		}
		if fields[0] == "MemTotal" {
			memTotal = value
		}
		if fields[0] == "MemAvailable" {
			memAvailable = value
		}
	}
	if memTotal == 0 {
		return Metric{}, errors.New("无法解析内存数据")
	}
	var rx, tx uint64
	for _, line := range netLines {
		if !strings.Contains(line, ":") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if strings.TrimSpace(parts[0]) == "lo" {
			continue
		}
		fields := strings.Fields(parts[1])
		if len(fields) < 9 {
			continue
		}
		rxValue, rxErr := parseUint(fields[0])
		txValue, txErr := parseUint(fields[8])
		if rxErr != nil || txErr != nil {
			return Metric{}, errors.New("无法解析网络数据")
		}
		rx += rxValue
		tx += txValue
	}
	diskFields := strings.Fields(diskLines[len(diskLines)-1])
	if len(diskFields) < 5 {
		return Metric{}, fmt.Errorf("无法解析磁盘数据: %q", diskLines[len(diskLines)-1])
	}
	diskPercent, err := strconv.ParseFloat(strings.TrimSuffix(diskFields[len(diskFields)-2], "%"), 64)
	if err != nil {
		return Metric{}, err
	}

	now := time.Now()
	current := counters{total: total, idle: idle, rx: rx, tx: tx, timestamp: now}
	c.mu.Lock()
	previous, exists := c.previous[hostID]
	c.previous[hostID] = current
	c.mu.Unlock()

	cpuPercent, rxMbps, txMbps := 0.0, 0.0, 0.0
	if exists && total > previous.total {
		totalDelta := total - previous.total
		idleDelta := idle - previous.idle
		cpuPercent = (1 - float64(idleDelta)/float64(totalDelta)) * 100
		elapsed := now.Sub(previous.timestamp).Seconds()
		if elapsed > 0 {
			if rx >= previous.rx {
				rxMbps = float64(rx-previous.rx) * 8 / elapsed / 1e6
			}
			if tx >= previous.tx {
				txMbps = float64(tx-previous.tx) * 8 / elapsed / 1e6
			}
		}
	}
	return Metric{
		Timestamp:     float64(now.UnixNano()) / 1e9,
		CPUPercent:    round2(clamp(cpuPercent, 0, 100)),
		MemoryPercent: round2(clamp((1-float64(memAvailable)/float64(memTotal))*100, 0, 100)),
		NetworkRXMbps: round3(rxMbps), NetworkTXMbps: round3(txMbps),
		DiskPercent: round2(clamp(diskPercent, 0, 100)),
	}, nil
}

func parseSystemInfo(output string) (SystemInfo, error) {
	lines := splitSections(output)["SYSINFO"]
	if len(lines) < 4 {
		return SystemInfo{}, errors.New("无法解析远端主机基本信息")
	}
	cores, err := strconv.Atoi(strings.TrimSpace(lines[1]))
	if err != nil {
		return SystemInfo{}, err
	}
	memoryKB, err := strconv.ParseInt(strings.TrimSpace(lines[2]), 10, 64)
	if err != nil {
		return SystemInfo{}, err
	}
	diskKB, err := strconv.ParseInt(strings.TrimSpace(lines[3]), 10, 64)
	if err != nil {
		return SystemInfo{}, err
	}
	return SystemInfo{
		Hostname: strings.TrimSpace(lines[0]), CPUCores: cores,
		MemoryBytes: memoryKB * 1024, DiskBytes: diskKB * 1024,
	}, nil
}

func (c *Collector) forget(hostID int) {
	c.mu.Lock()
	connection := c.connections[hostID]
	delete(c.previous, hostID)
	delete(c.connections, hostID)
	c.mu.Unlock()
	if connection != nil {
		connection.close()
	}
}

func (c *Collector) reset() {
	c.mu.Lock()
	connections := c.connections
	c.previous = make(map[int]counters)
	c.connections = make(map[int]*sshConnection)
	c.mu.Unlock()
	for _, connection := range connections {
		connection.close()
	}
}

func (c *Collector) pruneIdle() {
	c.mu.Lock()
	now := time.Now()
	idle := make([]*sshConnection, 0)
	for id, connection := range c.connections {
		if now.Sub(connection.lastUsed) > sshIdleTTL {
			delete(c.connections, id)
			idle = append(idle, connection)
		}
	}
	c.mu.Unlock()
	for _, connection := range idle {
		connection.close()
	}
}

func (c *Collector) connectionCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.connections)
}

func clamp(value, low, high float64) float64 {
	if value < low {
		return low
	}
	if value > high {
		return high
	}
	return value
}
func round2(value float64) float64 { return math.Round(value*100) / 100 }
func round3(value float64) float64 { return math.Round(value*1000) / 1000 }

type HostStatus struct {
	State        string      `json:"state"`
	Error        *string     `json:"error"`
	LastSuccess  float64     `json:"last_success,omitempty"`
	LastAttempt  float64     `json:"last_attempt,omitempty"`
	LatencyMS    int64       `json:"latency_ms,omitempty"`
	RetryAt      float64     `json:"retry_at,omitempty"`
	FailureCount int         `json:"failure_count,omitempty"`
	SystemInfo   *SystemInfo `json:"system_info,omitempty"`
}

type Poller struct {
	store      *Store
	metrics    *MetricStore
	collector  *Collector
	mu         sync.Mutex
	statuses   map[int]HostStatus
	systemInfo map[int]SystemInfo
	infoTimes  map[int]time.Time
	failures   map[int]int
	nextDue    map[int]time.Time
	hostLocks  map[int]*sync.Mutex
	revisions  map[int]uint64
	running    map[int]bool
	active     map[int]context.CancelFunc
	semaphore  chan struct{}
	queue      chan int
	wake       chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
	workers    int
	wg         sync.WaitGroup
	closeOnce  sync.Once
	generation uint64
	startedAt  time.Time
}

func newPoller(store *Store) *Poller {
	ctx, cancel := context.WithCancel(context.Background())
	workers := collectorConcurrency()
	return &Poller{
		store: store, metrics: newMetricStore(), collector: newCollector(),
		statuses: make(map[int]HostStatus), systemInfo: make(map[int]SystemInfo),
		infoTimes: make(map[int]time.Time), failures: make(map[int]int),
		nextDue: make(map[int]time.Time), hostLocks: make(map[int]*sync.Mutex),
		revisions: make(map[int]uint64), running: make(map[int]bool),
		active:    make(map[int]context.CancelFunc),
		semaphore: make(chan struct{}, workers), queue: make(chan int, workers*2),
		wake: make(chan struct{}, 1), ctx: ctx, cancel: cancel, workers: workers,
		startedAt: time.Now(),
	}
}

func collectorConcurrency() int {
	value := 8
	if raw := strings.TrimSpace(os.Getenv("HOSTWATCH_MAX_CONCURRENT")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 64 {
			log.Printf("HOSTWATCH_MAX_CONCURRENT=%q 无效，使用默认值 %d", raw, value)
		} else {
			value = parsed
		}
	}
	return value
}

func (p *Poller) start() {
	p.wg.Add(1 + p.workers)
	go p.loop()
	for i := 0; i < p.workers; i++ {
		go p.worker()
	}
}

func (p *Poller) close() {
	p.closeOnce.Do(func() {
		p.cancel()
		p.collector.reset()
	})
	p.wg.Wait()
}

func (p *Poller) wakeNow() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *Poller) loop() {
	defer p.wg.Done()
	for {
		wait := p.scheduleDue()
		timer := time.NewTimer(wait)
		select {
		case <-p.ctx.Done():
			timer.Stop()
			return
		case <-p.wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (p *Poller) scheduleDue() time.Duration {
	p.collector.pruneIdle()
	hosts, _ := p.store.listHosts(true)
	validHosts := make(map[int]struct{}, len(hosts))
	for _, host := range hosts {
		validHosts[host.ID] = struct{}{}
	}
	p.mu.Lock()
	for id := range p.hostLocks {
		if _, exists := validHosts[id]; !exists && !p.running[id] && p.active[id] == nil {
			delete(p.hostLocks, id)
			delete(p.revisions, id)
			delete(p.running, id)
			delete(p.nextDue, id)
		}
	}
	p.mu.Unlock()
	now := time.Now()
	interval := time.Duration(p.store.settings().RefreshInterval) * time.Second
	wait := time.Minute
	for _, host := range hosts {
		p.mu.Lock()
		due := p.nextDue[host.ID]
		if p.running[host.ID] {
			p.mu.Unlock()
			continue
		}
		if !due.IsZero() && now.Before(due) {
			p.mu.Unlock()
			if remaining := time.Until(due); remaining < wait {
				wait = remaining
			}
			continue
		}
		p.running[host.ID] = true
		// A provisional due time prevents duplicate queueing if a worker wakes
		// the dispatcher before the collection result is committed.
		p.nextDue[host.ID] = now.Add(interval)
		p.mu.Unlock()
		select {
		case p.queue <- host.ID:
		default:
			p.mu.Lock()
			p.running[host.ID] = false
			p.nextDue[host.ID] = time.Time{}
			p.mu.Unlock()
			if wait > 100*time.Millisecond {
				wait = 100 * time.Millisecond
			}
		}
	}
	if wait < 25*time.Millisecond {
		wait = 25 * time.Millisecond
	}
	return wait
}

func (p *Poller) worker() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case id := <-p.queue:
			p.collectHost(id, false)
			p.mu.Lock()
			p.running[id] = false
			p.mu.Unlock()
			p.wakeNow()
		}
	}
}

func (p *Poller) hostLock(id int) *sync.Mutex {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.hostLocks[id] == nil {
		p.hostLocks[id] = &sync.Mutex{}
	}
	return p.hostLocks[id]
}

func (p *Poller) collectHost(id int, force bool) HostStatus {
	lock := p.hostLock(id)
	lock.Lock()
	defer lock.Unlock()

	p.mu.Lock()
	if !force && time.Now().Before(p.nextDue[id]) && !p.running[id] {
		status := p.statuses[id]
		p.mu.Unlock()
		return status
	}
	p.mu.Unlock()

	host, ok := p.store.getHost(id)
	if !ok {
		return HostStatus{State: "missing", Error: stringPointer("主机不存在")}
	}
	settings := p.store.settings()
	now := time.Now()
	p.mu.Lock()
	generation := p.generation
	revision := p.revisions[id]
	info, hasInfo := p.systemInfo[id]
	infoTime := p.infoTimes[id]
	current := p.statuses[id]
	current.State, current.Error = "collecting", nil
	if hasInfo {
		current.SystemInfo = &info
	}
	p.statuses[id] = current
	collectionContext, cancel := context.WithTimeout(
		p.ctx, time.Duration(settings.SSHTimeout)*time.Second,
	)
	p.active[id] = cancel
	p.mu.Unlock()

	select {
	case p.semaphore <- struct{}{}:
	case <-collectionContext.Done():
		cancel()
		p.mu.Lock()
		delete(p.active, id)
		status := p.statuses[id]
		p.mu.Unlock()
		return status
	}
	started := time.Now()
	metric, collectedInfo, err := p.collector.collect(
		collectionContext, host, time.Duration(settings.SSHTimeout)*time.Second,
		!hasInfo || now.Sub(infoTime) >= systemInfoTTL,
	)
	<-p.semaphore
	cancel()
	latestSettings := p.store.settings()

	p.mu.Lock()
	delete(p.active, id)
	if generation != p.generation || revision != p.revisions[id] {
		p.mu.Unlock()
		p.collector.forget(id)
		if _, exists := p.store.getHost(id); !exists {
			return HostStatus{State: "missing", Error: stringPointer("主机不存在")}
		}
		return HostStatus{State: "pending"}
	}
	if p.ctx.Err() != nil {
		p.mu.Unlock()
		return HostStatus{State: "pending"}
	}
	if err == nil {
		if collectedInfo != nil {
			p.systemInfo[id] = *collectedInfo
			p.infoTimes[id] = time.Now()
			info = *collectedInfo
			hasInfo = true
		}
		metric = p.metrics.add(id, metric, latestSettings.HistoryMinutes)
		delete(p.failures, id)
		p.nextDue[id] = time.Now().Add(time.Duration(latestSettings.RefreshInterval) * time.Second)
		status := HostStatus{
			State: "online", LastSuccess: metric.Timestamp,
			LastAttempt: metric.Timestamp, LatencyMS: time.Since(started).Milliseconds(),
		}
		if hasInfo {
			status.SystemInfo = &info
		}
		p.statuses[id] = status
		p.mu.Unlock()
		return status
	}
	failures := p.failures[id] + 1
	p.failures[id] = failures
	backoff := retryBackoff(latestSettings.RefreshInterval, failures)
	retryAt := time.Now().Add(backoff)
	p.nextDue[id] = retryAt
	message := err.Error()
	status := HostStatus{
		State: "error", Error: &message, LastAttempt: float64(time.Now().UnixNano()) / 1e9,
		LastSuccess: current.LastSuccess, RetryAt: float64(retryAt.UnixNano()) / 1e9,
		FailureCount: failures,
	}
	if hasInfo {
		status.SystemInfo = &info
	}
	p.statuses[id] = status
	if failures == 1 || failures == 2 || failures == 4 || failures%10 == 0 {
		log.Printf("host %d collection failed (%d failures, retry in %s): %v", id, failures, backoff, err)
	}
	p.mu.Unlock()
	return status
}

func retryBackoff(refreshInterval, failures int) time.Duration {
	base := time.Duration(refreshInterval) * time.Second
	if base < 15*time.Second {
		base = 15 * time.Second
	}
	if failures < 1 {
		failures = 1
	}
	exponent := failures - 1
	if exponent > 5 {
		exponent = 5
	}
	backoff := base * time.Duration(1<<exponent)
	if backoff > maxBackoff {
		return maxBackoff
	}
	return backoff
}

func (p *Poller) status(id int) HostStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	status, ok := p.statuses[id]
	if !ok {
		status = HostStatus{State: "pending"}
	}
	if info, exists := p.systemInfo[id]; exists {
		status.SystemInfo = &info
	}
	return status
}

func (p *Poller) statusSnapshot(hostIDs []int) map[int]HostStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	statuses := make(map[int]HostStatus, len(hostIDs))
	for _, id := range hostIDs {
		status, ok := p.statuses[id]
		if !ok {
			status = HostStatus{State: "pending"}
		}
		if storedInfo, exists := p.systemInfo[id]; exists {
			info := storedInfo
			status.SystemInfo = &info
		}
		statuses[id] = status
	}
	return statuses
}

func (p *Poller) removeHost(id int) {
	p.mu.Lock()
	p.revisions[id]++
	if cancel := p.active[id]; cancel != nil {
		cancel()
	}
	delete(p.statuses, id)
	delete(p.systemInfo, id)
	delete(p.infoTimes, id)
	delete(p.failures, id)
	delete(p.nextDue, id)
	p.mu.Unlock()
	p.metrics.remove(id)
	p.collector.forget(id)
}

func (p *Poller) hostUpdated(id int) {
	p.mu.Lock()
	p.revisions[id]++
	if cancel := p.active[id]; cancel != nil {
		cancel()
	}
	delete(p.systemInfo, id)
	delete(p.infoTimes, id)
	delete(p.failures, id)
	delete(p.nextDue, id)
	p.statuses[id] = HostStatus{State: "pending"}
	p.mu.Unlock()
	p.collector.forget(id)
	p.wakeNow()
}

func (p *Poller) rescheduleAll() {
	p.mu.Lock()
	for id := range p.nextDue {
		p.nextDue[id] = time.Time{}
	}
	p.mu.Unlock()
	p.wakeNow()
}

func (p *Poller) requestRefresh(id int) HostStatus {
	p.mu.Lock()
	p.nextDue[id] = time.Time{}
	status := p.statuses[id]
	if !p.running[id] {
		status.State = "pending"
		status.Error = nil
		p.statuses[id] = status
	}
	p.mu.Unlock()
	p.wakeNow()
	return status
}

func (p *Poller) reset() {
	p.mu.Lock()
	p.generation++
	for id, cancel := range p.active {
		p.revisions[id]++
		cancel()
	}
	p.statuses = make(map[int]HostStatus)
	p.systemInfo = make(map[int]SystemInfo)
	p.infoTimes = make(map[int]time.Time)
	p.failures = make(map[int]int)
	p.nextDue = make(map[int]time.Time)
	p.mu.Unlock()
	p.metrics.reset()
	p.collector.reset()
	p.wakeNow()
}

func (p *Poller) stats() (running int, queued int, workers int, uptime time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, active := range p.running {
		if active {
			running++
		}
	}
	return running, len(p.queue), p.workers, time.Since(p.startedAt)
}

func stringPointer(value string) *string { return &value }

type App struct {
	store  *Store
	poller *Poller
}

type DashboardHost struct {
	PublicHost
	Status HostStatus `json:"status"`
}

func (a *App) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", a.index)
	mux.HandleFunc("GET /static/style.css", func(w http.ResponseWriter, r *http.Request) {
		serveAsset(w, r, "text/css; charset=utf-8", styleCSS, styleCSSGzip, "public, max-age=0, must-revalidate")
	})
	mux.HandleFunc("GET /static/app.js", func(w http.ResponseWriter, r *http.Request) {
		serveAsset(w, r, "application/javascript; charset=utf-8", appJS, appJSGzip, "public, max-age=0, must-revalidate")
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "version": version})
	})
	mux.HandleFunc("GET /api/status", a.runtimeStatus)
	mux.HandleFunc("GET /api/hosts", a.listHosts)
	mux.HandleFunc("POST /api/hosts", a.createHost)
	mux.HandleFunc("PATCH /api/hosts/{id}", a.updateHost)
	mux.HandleFunc("DELETE /api/hosts/{id}", a.deleteHost)
	mux.HandleFunc("POST /api/hosts/reorder", a.reorderHosts)
	mux.HandleFunc("POST /api/hosts/{id}/refresh", a.refreshHost)
	mux.HandleFunc("GET /api/hosts/{id}/metrics", a.hostMetrics)
	mux.HandleFunc("GET /api/metrics", a.allMetrics)
	mux.HandleFunc("GET /api/settings", a.getSettings)
	mux.HandleFunc("PUT /api/settings", a.updateSettings)
	mux.HandleFunc("GET /api/config", a.exportConfig)
	mux.HandleFunc("PUT /api/config", a.importConfig)
	mux.HandleFunc("GET /api/dashboard", a.dashboard)
	mux.HandleFunc("GET /api/snapshot", a.snapshot)
	return requestLogger(gzipAPIResponses(mux))
}

func (a *App) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	serveAsset(w, r, "text/html; charset=utf-8", indexHTML, indexHTMLGzip, "no-cache")
}

func serveAsset(w http.ResponseWriter, r *http.Request, contentType string, content string, compressed []byte, cacheControl string) {
	etag := `"` + version + `-` + strconv.Itoa(len(content)) + `"`
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", cacheControl)
	w.Header().Set("ETag", etag)
	w.Header().Set("Vary", "Accept-Encoding")
	acceptsGzip := strings.Contains(r.Header.Get("Accept-Encoding"), "gzip")
	if acceptsGzip {
		w.Header().Set("Content-Encoding", "gzip")
	}
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	if acceptsGzip {
		_, _ = w.Write(compressed)
		return
	}
	_, _ = io.WriteString(w, content)
}

func (a *App) listHosts(w http.ResponseWriter, r *http.Request) {
	_, hosts := a.store.listHosts(false)
	writeJSON(w, http.StatusOK, hosts)
}

func decodeJSON(r *http.Request, target any) error {
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxJSONBodyBytes+1))
	if err != nil {
		return err
	}
	if len(raw) > maxJSONBodyBytes {
		return errors.New("请求内容超过 1 MiB 限制")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("请求只能包含一个 JSON 对象")
	}
	return nil
}

func (a *App) createHost(w http.ResponseWriter, r *http.Request) {
	var host Host
	if err := decodeJSON(r, &host); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	created, err := a.store.createHost(host)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err)
		return
	}
	a.poller.wakeNow()
	writeJSON(w, http.StatusCreated, created)
}

func parseID(r *http.Request) (int, error) {
	return strconv.Atoi(r.PathValue("id"))
}

func (a *App) updateHost(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var patch HostPatch
	if err := decodeJSON(r, &patch); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	updated, err := a.store.updateHost(id, patch)
	if errors.Is(err, os.ErrNotExist) {
		writeError(w, http.StatusNotFound, errors.New("主机不存在"))
		return
	}
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err)
		return
	}
	a.poller.hostUpdated(id)
	writeJSON(w, http.StatusOK, updated)
}

func (a *App) deleteHost(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	deleted, err := a.store.deleteHost(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if !deleted {
		writeError(w, http.StatusNotFound, errors.New("主机不存在"))
		return
	}
	a.poller.removeHost(id)
	w.WriteHeader(http.StatusNoContent)
}

func (a *App) reorderHosts(w http.ResponseWriter, r *http.Request) {
	var body struct {
		HostIDs []int `json:"host_ids"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := a.store.reorder(body.HostIDs); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *App) refreshHost(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if _, ok := a.store.getHost(id); !ok {
		writeError(w, http.StatusNotFound, errors.New("主机不存在"))
		return
	}
	writeJSON(w, http.StatusAccepted, a.poller.requestRefresh(id))
}

func parseMetricQuery(r *http.Request) (*float64, *uint64, int, error) {
	var sinceTimestamp *float64
	if raw := r.URL.Query().Get("since"); raw != "" {
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil || value < 0 {
			return nil, nil, 0, errors.New("since 参数无效")
		}
		sinceTimestamp = &value
	}
	var sinceSequence *uint64
	if raw := r.URL.Query().Get("since_seq"); raw != "" {
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return nil, nil, 0, errors.New("since_seq 参数无效")
		}
		sinceSequence = &value
	}
	limit := defaultChartPoints
	if raw := r.URL.Query().Get("max_points"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 2 || value > maxChartPoints {
			return nil, nil, 0, errors.New("max_points 参数无效")
		}
		limit = value
	}
	return sinceTimestamp, sinceSequence, limit, nil
}

func (a *App) hostMetrics(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if _, ok := a.store.getHost(id); !ok {
		writeError(w, http.StatusNotFound, errors.New("主机不存在"))
		return
	}
	sinceTimestamp, sinceSequence, limit, err := parseMetricQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	settings := a.store.settings()
	metrics, sequence := a.poller.metrics.getSnapshot(id, settings.HistoryMinutes, sinceTimestamp, sinceSequence, limit)
	w.Header().Set("X-HostWatch-Sequence", strconv.FormatUint(sequence, 10))
	writeJSON(w, http.StatusOK, metrics)
}

func (a *App) allMetrics(w http.ResponseWriter, r *http.Request) {
	sinceTimestamp, sinceSequence, limit, err := parseMetricQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	privateHosts, _ := a.store.listHosts(true)
	settings := a.store.settings()
	hostIDs := make([]int, 0, len(privateHosts))
	for _, host := range privateHosts {
		hostIDs = append(hostIDs, host.ID)
	}
	metrics, nextSequence := a.poller.metrics.getAll(
		hostIDs, settings.HistoryMinutes, sinceTimestamp, sinceSequence, limit,
	)
	writeJSON(w, http.StatusOK, map[string]any{
		"metrics": metrics, "next_sequence": nextSequence,
		"server_time": float64(time.Now().UnixNano()) / 1e9,
	})
}

func (a *App) getSettings(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.store.settings())
}

func (a *App) updateSettings(w http.ResponseWriter, r *http.Request) {
	var settings Settings
	if err := decodeJSON(r, &settings); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := a.store.updateSettings(settings); err != nil {
		writeError(w, http.StatusUnprocessableEntity, err)
		return
	}
	a.poller.metrics.pruneAll(settings.HistoryMinutes)
	a.poller.rescheduleAll()
	writeJSON(w, http.StatusOK, settings)
}

func (a *App) exportConfig(w http.ResponseWriter, r *http.Request) {
	raw, err := a.store.exportConfig()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="hostwatch-config.json"`)
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(raw); err != nil {
		log.Printf("write config export: %v", err)
	}
}

func (a *App) importConfig(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxConfigBytes+1))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if len(raw) > maxConfigBytes {
		writeError(w, http.StatusRequestEntityTooLarge, errors.New("配置文件过大"))
		return
	}
	if err := a.store.importConfig(raw); err != nil {
		writeError(w, http.StatusUnprocessableEntity, err)
		return
	}
	a.poller.reset()
	_, hosts := a.store.listHosts(false)
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok", "host_count": len(hosts), "settings": a.store.settings(),
	})
}

func (a *App) dashboard(w http.ResponseWriter, r *http.Request) {
	_, hosts := a.store.listHosts(false)
	hostIDs := make([]int, len(hosts))
	for index, host := range hosts {
		hostIDs[index] = host.ID
	}
	statuses := a.poller.statusSnapshot(hostIDs)
	items := make([]DashboardHost, 0, len(hosts))
	for _, host := range hosts {
		items = append(items, DashboardHost{PublicHost: host, Status: statuses[host.ID]})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"settings": a.store.settings(), "hosts": items,
		"server_time": float64(time.Now().UnixNano()) / 1e9,
	})
}

func (a *App) snapshot(w http.ResponseWriter, r *http.Request) {
	sinceTimestamp, sinceSequence, limit, err := parseMetricQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	_, hosts := a.store.listHosts(false)
	items := make([]DashboardHost, 0, len(hosts))
	hostIDs := make([]int, 0, len(hosts))
	for _, host := range hosts {
		hostIDs = append(hostIDs, host.ID)
	}
	statuses := a.poller.statusSnapshot(hostIDs)
	for _, host := range hosts {
		items = append(items, DashboardHost{PublicHost: host, Status: statuses[host.ID]})
	}
	settings := a.store.settings()
	metrics, nextSequence := a.poller.metrics.getAll(
		hostIDs, settings.HistoryMinutes, sinceTimestamp, sinceSequence, limit,
	)
	writeJSON(w, http.StatusOK, map[string]any{
		"settings": settings, "hosts": items, "metrics": metrics,
		"next_sequence": nextSequence,
		"server_time":   float64(time.Now().UnixNano()) / 1e9,
	})
}

func (a *App) runtimeStatus(w http.ResponseWriter, r *http.Request) {
	_, hosts := a.store.listHosts(false)
	hostIDs := make([]int, len(hosts))
	for index, host := range hosts {
		hostIDs[index] = host.ID
	}
	statuses := a.poller.statusSnapshot(hostIDs)
	online, failed := 0, 0
	for _, host := range hosts {
		switch statuses[host.ID].State {
		case "online":
			online++
		case "error":
			failed++
		}
	}
	points, metricHosts, sequence := a.poller.metrics.stats()
	running, queued, workers, uptime := a.poller.stats()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	writeJSON(w, http.StatusOK, map[string]any{
		"version": version, "uptime_seconds": int64(uptime.Seconds()),
		"hosts": map[string]int{"total": len(hosts), "online": online, "error": failed},
		"collector": map[string]int{
			"workers": workers, "scheduled": running, "queued": queued,
			"ssh_connections": a.poller.collector.connectionCount(),
		},
		"metrics": map[string]any{"points": points, "hosts": metricHosts, "sequence": sequence},
		"runtime": map[string]any{
			"goroutines": runtime.NumGoroutine(), "alloc_bytes": memory.Alloc,
			"sys_bytes": memory.Sys,
		},
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"detail": err.Error()})
}

var gzipWriterPool = sync.Pool{New: func() any {
	writer, _ := gzip.NewWriterLevel(io.Discard, gzip.BestSpeed)
	return writer
}}

type gzipResponseWriter struct {
	http.ResponseWriter
	writer      *gzip.Writer
	wroteHeader bool
}

func (writer *gzipResponseWriter) WriteHeader(status int) {
	if writer.wroteHeader {
		return
	}
	writer.wroteHeader = true
	writer.Header().Set("Content-Encoding", "gzip")
	writer.Header().Add("Vary", "Accept-Encoding")
	writer.Header().Del("Content-Length")
	writer.ResponseWriter.WriteHeader(status)
}

func (writer *gzipResponseWriter) Write(content []byte) (int, error) {
	if !writer.wroteHeader {
		writer.WriteHeader(http.StatusOK)
	}
	return writer.writer.Write(content)
}

func gzipAPIResponses(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		compressible := r.Method == http.MethodGet && (r.URL.Path == "/api/snapshot" ||
			r.URL.Path == "/api/metrics" || r.URL.Path == "/api/dashboard" ||
			r.URL.Path == "/api/config")
		if !compressible || !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}
		gzipWriter := gzipWriterPool.Get().(*gzip.Writer)
		gzipWriter.Reset(w)
		wrapped := &gzipResponseWriter{ResponseWriter: w, writer: gzipWriter}
		next.ServeHTTP(wrapped, r)
		if err := gzipWriter.Close(); err != nil {
			log.Printf("compress response: %v", err)
		}
		gzipWriter.Reset(io.Discard)
		gzipWriterPool.Put(gzipWriter)
	})
}

func requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		next.ServeHTTP(w, r)
		quietPath := r.URL.Path == "/health" || r.URL.Path == "/api/status" ||
			r.URL.Path == "/api/dashboard" || r.URL.Path == "/api/metrics" ||
			r.URL.Path == "/api/snapshot"
		if !quietPath {
			log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(started).Round(time.Millisecond))
		}
	})
}

func mustDecodeAsset(encoded string) (string, []byte) {
	compressed, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		panic(err)
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		panic(err)
	}
	defer reader.Close()
	raw, err := io.ReadAll(reader)
	if err != nil {
		panic(err)
	}
	return string(raw), compressed
}

func runSelfTest() error {
	if !strings.Contains(indexHTML, "HostWatch") || len(styleCSS) < 100 || len(appJS) < 100 {
		return errors.New("embedded web assets are invalid")
	}
	sample := `__SYSINFO__
test-host
4
16777216
104857600
__STAT__
cpu 100 0 50 850 0 0 0 0
__MEM__
MemTotal: 1000 kB
MemAvailable: 400 kB
__NET__
lo: 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0
eth0: 1000 0 0 0 0 0 0 0 2000 0 0 0 0 0 0 0
__DISK__
/dev/sda1 100000 42000 58000 42% /
`
	collector := newCollector()
	metric, err := collector.parseMetric(1, sample)
	if err != nil || metric.MemoryPercent != 60 || metric.DiskPercent != 42 {
		return fmt.Errorf("metric parser self-test failed: %v", err)
	}
	info, err := parseSystemInfo(sample)
	if err != nil || info.CPUCores != 4 || info.Hostname != "test-host" {
		return fmt.Errorf("system info parser self-test failed: %v", err)
	}
	metrics := newMetricStore()
	now := float64(time.Now().UnixNano()) / 1e9
	for i := 0; i < 100; i++ {
		metrics.add(1, Metric{Timestamp: now + float64(i)}, 60)
	}
	if len(metrics.get(1, 60, nil, 10)) != 10 {
		return errors.New("metric downsampling self-test failed")
	}
	temp, err := os.MkdirTemp("", "hostwatch-self-test-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temp)
	store, err := newStore(filepath.Join(temp, "config.json"))
	if err != nil {
		return err
	}
	password := "plain"
	_, err = store.createHost(Host{Name: "test", Address: "127.0.0.1", Port: 22, Username: "root", AuthType: "password", Password: &password})
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(filepath.Join(temp, "config.json"))
	if err != nil || !strings.Contains(string(raw), "\"plain\"") {
		return errors.New("JSON store self-test failed")
	}
	return nil
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func listenAddress() string {
	return net.JoinHostPort(os.Getenv("HOSTWATCH_HOST"), envOrDefault("HOSTWATCH_PORT", "8000"))
}

func main() {
	selfTest := flag.Bool("self-test", false, "run built-in checks and exit")
	versionFlag := flag.Bool("version", false, "print version and exit")
	checkConfig := flag.Bool("check-config", false, "validate config and exit")
	listenFlag := flag.String("listen", "", "HTTP listen address, for example :8000 or [::1]:8000")
	dataDirFlag := flag.String("data-dir", "", "configuration directory (overrides HOSTWATCH_DATA_DIR)")
	flag.Parse()
	if *versionFlag {
		fmt.Println(version)
		return
	}
	if *selfTest {
		if err := runSelfTest(); err != nil {
			log.Fatal(err)
		}
		fmt.Println("HostWatch self-test passed")
		return
	}

	dataDir := *dataDirFlag
	if dataDir == "" {
		dataDir = envOrDefault("HOSTWATCH_DATA_DIR", "data")
	}
	store, err := newStore(filepath.Join(dataDir, "config.json"))
	if err != nil {
		log.Fatal(err)
	}
	if *checkConfig {
		_, hosts := store.listHosts(false)
		fmt.Printf("HostWatch config OK (%d hosts)\n", len(hosts))
		return
	}
	poller := newPoller(store)
	poller.start()
	defer poller.close()

	address := listenAddress()
	if *listenFlag != "" {
		address = *listenFlag
	}
	server := &http.Server{
		Addr: address, Handler: (&App{store: store, poller: poller}).routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	serverErrors := make(chan error, 1)
	go func() {
		displayAddress := address
		if strings.HasPrefix(displayAddress, ":") {
			displayAddress = "[::]" + displayAddress
		}
		log.Printf("HostWatch %s listening on http://%s", version, displayAddress)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrors <- err
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	var serveErr error
	select {
	case <-signals:
		log.Printf("收到退出信号，正在停止服务")
	case err := <-serverErrors:
		serveErr = err
	}
	signal.Stop(signals)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("shutdown: %v", err)
	}
	poller.close()
	if serveErr != nil {
		log.Fatalf("HTTP 服务异常退出: %v", serveErr)
	}
}
