import math


class LCC:
    """
    Lambert Conformal Conic Projection
    """

    PI = math.asin(1.0) * 2.0
    DEGRAD = PI / 180.0
    RADDEG = 180.0 / PI

    def __init__(
        self,
        Re: float = 6371.00877,
        grid: float = 5.0,
        standard_lat_1: float = 30.0,
        standard_lat_2: float = 60.0,
        base_lon: float = 126.0,
        base_lat: float = 38.0,
        base_x: int = 210,
        base_y: int = 675,
    ):
        """
        Lambert Conformal Conic Projection 를 수행하기 위한 클래스
        WGS 좌표계와 Grid 좌표계간의 전환을 위한 메소드를 소유하고 있습니다.

        Args:
            - Re : 지도 반경
            - grid : 격자 간격 (km)
            - standard_lat_1 : 표준위도 1
            - standard_lat_2 : 표준위도 2
            - olon : 기준점 경도
            - olat : 기준점 위도
            - xo : 기준점 x 좌표
            - yo : 기준점 y 좌표
        """
        self.Re = Re  ##  지도반경
        self.grid = grid  ##  격자간격 (km)
        self.slat1 = standard_lat_1 * LCC.DEGRAD  ##  표준위도 1
        self.slat2 = standard_lat_2 * LCC.DEGRAD  ##  표준위도 2
        self.olon = base_lon * LCC.DEGRAD  ##  기준점 경도
        self.olat = base_lat * LCC.DEGRAD  ##  기준점 위도
        self.xo = base_x / grid  ##  기준점 X좌표
        self.yo = base_y / grid  ##  기준점 Y좌표

        self.re = Re / grid

        sn = math.tan(LCC.PI * 0.25 + self.slat2 * 0.5) / math.tan(
            LCC.PI * 0.25 + self.slat1 * 0.5
        )
        sn = math.log(math.cos(self.slat1) / math.cos(self.slat2)) / math.log(sn)
        self.sn = sn

        sf = math.tan(LCC.PI * 0.25 + self.slat1 * 0.5)
        sf = math.pow(sf, sn) * math.cos(self.slat1) / sn
        self.sf = sf

        ro = math.tan(LCC.PI * 0.25 + self.olat * 0.5)
        ro = self.re * sf / math.pow(ro, sn)
        self.ro = ro

    def to_grid(self, lat: float, lon: float) -> tuple:
        """(위도,경도)를 Lambert Conformal Conic Projection 하여 (x,y) 로 변환

        Args:
            lat (float): 변환하려는 위도
            lon (float): 변환하려는 경도

        Returns:
            tuple: Lambert Conformal Conic grid x, y 좌표
        """
        ra = math.tan(LCC.PI * 0.25 + lat * LCC.DEGRAD * 0.5)
        ra = self.re * self.sf / pow(ra, self.sn)
        theta = lon * LCC.DEGRAD - self.olon
        if theta > LCC.PI:
            theta -= 2.0 * LCC.PI
        if theta < -LCC.PI:
            theta += 2.0 * LCC.PI
        theta *= self.sn
        x = (ra * math.sin(theta)) + self.xo
        y = (self.ro - ra * math.cos(theta)) + self.yo
        x = int(x + 1.5)
        y = int(y + 1.5)
        return x, y

    def to_map(self, x: int, y: int) -> tuple:
        """(x,y)를 Lambert Conformal Conic Projection 하여 (위도,경도) 로 변환

        Args:
            x (int): x축 좌표
            y (int): y축 좌표

        Returns:
            tuple: WGS 위도, 경도
        """
        x = x - 1
        y = y - 1
        xn = x - self.xo
        yn = self.ro - y + self.yo
        ra = math.sqrt(xn * xn + yn * yn)
        if self.sn < 0.0:
            ra = -ra
        alat = math.pow((self.re * self.sf / ra), (1.0 / self.sn))
        alat = 2.0 * math.atan(alat) - LCC.PI * 0.5
        if math.fabs(xn) <= 0.0:
            theta = 0.0
        else:
            if math.fabs(yn) <= 0.0:
                theta = LCC.PI * 0.5
                if xn < 0.0:
                    theta = -theta
            else:
                theta = math.atan2(xn, yn)
        alon = theta / self.sn + self.olon
        lat = alat * LCC.RADDEG
        lon = alon * LCC.RADDEG

        return lat, lon
