import org.apache.commons.lang3.RandomStringUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;
import java.util.UUID;

public class Event {
    private String product;
    private String price;
    private String date;
    private String category;
    private String ip;

    public Event() {
        Random r = new Random();
        this.product =  products[r.nextInt(14)];
               // RandomStringUtils.randomAlphabetic(7);
        this.price =String.valueOf(( new BigDecimal(10 + (2000 - 10) * r.nextDouble()).setScale(2, RoundingMode.UP)));
        this.date = dates[r.nextInt(6)]+time[r.nextInt(8)];
        this.category = categorys[r.nextInt(6)];
        this.ip = r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256);;
    }

    static String[] dates = new String[]{"2019-09-07","2019-09-06","2019-09-05","2019-09-04","2019-09-03","2019-09-02","2019-09-01"};
    static String[] categorys = new String[]{"electronics","toys","books","gifts","computers","clothes","shoes"};
    static String[] time = new String[]{"T09:45:00.000+02:00",
            "T01:30:00.000+02:00",
            "T07:45:00.000+02:00",
            "T22:12:00.000+02:00",
            "T19:13:00.000+02:00",
            "T17:56:00.000+02:00",
            "T23:12:00.000+02:00",
            "T16:13:00.000+02:00",
            "T12:56:00.000+02:00",
            "T20:00:00.000+02:00"};
    static String[] products = new String[]{"product1","product2","product3","product4","product5","product7","product7"
            ,"product8","product9","product10","product11","product12","product13","product14","product15"};

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    @Override
    public String toString() {
        return product+"\t"+price+"\t"+date+"\t"+category+"\t"+ip+"\n";
//        StringBuilder stringBuilder = new StringBuilder();
//        stringBuilder.append(product)
//                .append("\t")
//                .append(price)
//                .append("\t")
//                .append(date)
//                .append("\t")
//                .append(category)
//                .append("\t")
//                .append(ip)
//                .append("\n");
//        return stringBuilder.toString();
    }
}
