import com.nhmTri.FPT_project.ReadJsonFromDate;
import com.nhmTri.FPT_project.SparkProcessing;
import org.apache.hadoop.security.SaslOutputStream;
import org.apache.spark.annotation.Private;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


//import static org.apache.commons.lang3.time.DateUtils.parseDate;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.

public class Main {
    public static void main(String[] args) throws Exception {
        String folderPath = getFolderPath();
        System.out.println("1. Nhập ngày bắt đầu ---------------------------");
        String startDateStr = getValidDate("Nhập ngày bắt đầu (dd/MM/yyyy): ");
        Date startDate = parseDateCustom(startDateStr);
        System.out.println("2. Nhập ngày kết thúc ---------------------------");
        String endDateStr;
        Date endDate;
        while (true) {
            endDateStr = getValidDate("Nhập ngày kết thúc (dd/MM/yyyy): ");
            endDate = parseDateCustom(endDateStr);
            if (endDate != null && !endDate.before(startDate)) break;

            System.out.println("❌ Ngày kết thúc không được nhỏ hơn ngày bắt đầu! Vui lòng nhập lại.");
        }
        ReadJsonFromDate reader = new ReadJsonFromDate(endDateStr, folderPath, startDateStr);
        List<Map<String, String>> fileList = reader.readAllFile();

        SparkProcessing processor = new SparkProcessing(fileList);
        Dataset<Row> df = processor.createDataFrame(fileList);
        df = processor.extractingColumns(df);
        df = processor.dataFrameTransform(df);
        df = processor.gettingStasteUDF(df);
        df = processor.gettingActiveness(df);
        df.show(20);

    }

    public static String getFolderPath() throws Exception {
        Properties props = new Properties();
        props.load(Main.class.getClassLoader().getResourceAsStream("config.properties"));
        return props.getProperty("folderPath");
    }

    // ✅ Hàm nhập ngày hợp lệ từ bàn phím
    public static String getValidDate(String message) {
        Scanner scanner = new Scanner(System.in);
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        sdf.setLenient(false);

        while (true) {
            System.out.print(message);
            String input = scanner.nextLine();
            try {
                sdf.parse(input);
                return input;
            } catch (ParseException e) {
                System.out.println(" Ngày không hợp lệ! Vui lòng nhập đúng định dạng (dd/MM/yyyy). Ví dụ: 21/06/2025");
            }
        }
    }

    // ✅ Hàm chuyển từ chuỗi sang Date (an toàn)
    public static Date parseDateCustom(String dateStr) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
            sdf.setLenient(false);
            return sdf.parse(dateStr);
        } catch (ParseException e) {
            System.err.println(" Không thể phân tích ngày: " + dateStr);
            return null;
        }
    }
}