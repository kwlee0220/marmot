package marmot.support;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;

import org.locationtech.jts.geom.Coordinate;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class NaverGeoCoder implements GeoCoder {
    private static final String clientId = "NSaCK6qsDZHobuCtUsDG";//애플리케이션 클라이언트 아이디값";
    private static final String clientSecret = "ypkJQ1hzt0";//애플리케이션 클라이언트 시크릿값";

    public static void main(String[] args) {
        System.out.println(new NaverGeoCoder().getWgs84Location("대전광역시 유성구 가정로 168"));
    }

    //일일 최대 200,000 호출 가능
    public List<Coordinate> getWgs84Location(String address) {
        try {
            String addr = URLEncoder.encode(address, "UTF-8");
//            String apiURL = "https://openapi.naver.com/v1/map/geocode?query=" + addr; //json
            String apiURL = "https://openapi.naver.com/v1/map/geocode.xml?query=" + addr; // xml
            URL url = new URL(apiURL);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");
            con.setRequestProperty("X-Naver-Client-Id", clientId);
            con.setRequestProperty("X-Naver-Client-Secret", clientSecret);
            int responseCode = con.getResponseCode();
            BufferedReader br;
            if (responseCode == 200) { // 정상 호출
                Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(con.getInputStream());
                document.getDocumentElement().normalize();

                NodeList items = document.getElementsByTagName("item");

                List<Coordinate> geoCodes = new ArrayList<Coordinate>(items.getLength());

                for (int i = 0; i < items.getLength(); i++) {
                    Node item = items.item(i);

                    if (item.getNodeType() == Node.ELEMENT_NODE) {
                        Element eItem = (Element) item;
//                        String retAddress = eItem.getElementsByTagName("address").item(0).getTextContent();
//                        boolean isRoadAddress = Boolean.parseBoolean(eItem.getElementsByTagName("address").item(0).getTextContent());
                        Element point = (Element) eItem.getElementsByTagName("point").item(0);

                        double x = Double.parseDouble(point.getElementsByTagName("x").item(0).getTextContent());
                        double y = Double.parseDouble(point.getElementsByTagName("y").item(0).getTextContent());
                        geoCodes.add(new Coordinate(x, y));
                    }
                }

                return geoCodes;
            } else {  // 에러 발생
                br = new BufferedReader(new InputStreamReader(con.getErrorStream()));
                String inputLine;
                StringBuffer response = new StringBuffer();
                while ((inputLine = br.readLine()) != null) {
                    response.append(inputLine);
                }
                br.close();
//                System.err.println(response.toString());
            }
        } catch (Exception e) {
            System.err.println(e);
        }
        return Collections.EMPTY_LIST;
    }

}
