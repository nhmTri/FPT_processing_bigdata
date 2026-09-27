package com.nhmTri.FPT_project;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;

public class ReadJsonFromDate {
    private static final Logger logger = LoggerFactory.getLogger(ReadJsonFromDate.class);
    public String startDateStr;
    public String endDateStr;
    public String folderPath;
    List<Map<String,String>> fileList = new ArrayList<>();


    public ReadJsonFromDate(String endDateStr, String folderPath, String startDateStr) {
        this.endDateStr = endDateStr;
        this.folderPath = folderPath;
        this.startDateStr = startDateStr;
    }

    public List<Map<String,String>> readAllFile() throws Exception {
        SimpleDateFormat inputFormat = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat fileFormat = new SimpleDateFormat("yyyyMMdd");

        Date startDate = inputFormat.parse(startDateStr);
        Date endDate = inputFormat.parse(endDateStr);

        if (startDate.after(endDate)) {
            throw new Exception("Ngày bắt đầu phải trước học bằng ngày kết thúc");
        }

//      List<String> fileList = new ArrayList<>();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(startDate);

        while (calendar.getTime().before(endDate)) {
            String date = fileFormat.format(calendar.getTime());
            String fileName = fileFormat.format(calendar.getTime()) + ".json";
            String filePath = folderPath + "\\" + fileName;

            File file = new File(filePath);
            if (file.exists()) {
                Map<String,String> map = new HashMap<>();
                map.put("path", filePath);
                map.put("date", date);
                fileList.add(map);
                logger.info("Đã thêm file: {}", filePath);
            } else {
                logger.warn("Không tim thấy file {}", filePath);
            }
            calendar.add(Calendar.DATE, 1);
        }
        if (fileList.size() == 0) {
            logger.warn("Khong tim thay file Json");
        }

        return fileList;

    }

    public void getStartDateStr() {
    }

    public void getEndDateStr() {
    }

    public void getFolderPath() {
    }
}

