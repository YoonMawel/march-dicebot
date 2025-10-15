# run_dice_marchend.py
import os, sys

# === 1) 패키지 위치 잡기 =========================================
# 이 런처 파일과 같은 폴더에 dice_marchend/ 가 있다면:
BASE = os.path.dirname(os.path.abspath(__file__))
CANDIDATES = [
    BASE,                                   # ./dice_marchend
    os.path.join(BASE, "src")         # ./src/dice_marchend (있을 경우)
]
for path in CANDIDATES:
    if path and os.path.isdir(os.path.join(path, "dice_marchend")):
        if path not in sys.path:
            sys.path.insert(0, path)
        break
else:
    raise RuntimeError(
        "dice_marchend 패키지를 찾을 수 없습니다. "
        "run_dice_marchend.py와 같은 폴더에 dice_marchend/를 두거나, "
        "환경변수 DICE_MARCHEND_PATH 로 패키지 상위 폴더를 지정하세요."
    )

# === 2) 실제 실행 =================================================
from dice_marchend.runner import main  # 패키지 내부 runner.py 의 main()
if __name__ == "__main__":
    main()
