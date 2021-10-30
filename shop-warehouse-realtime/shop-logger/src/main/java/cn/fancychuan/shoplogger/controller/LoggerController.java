package cn.fancychuan.shoplogger.controller;


import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

//@RestController = @Controller+@ResponseBody
@RestController //表示返回普通对象而不是页面
@Slf4j
public class LoggerController {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("applog")
    public String getLogger(@RequestParam("param") String jsonStr) {
        // 落盘
        log.info(jsonStr);
        // 写入kafka
        kafkaTemplate.send("ods_base_log", jsonStr);
        return "success";
    }




    @RequestMapping("test1")
    public String test1() {
        System.out.println("1111");
        return "success";
    }

    @RequestMapping("test2")
    public String test2(@RequestParam("names") String name, @RequestParam("age") int age) {
        System.out.println(name + ":" + age);
        return "success";
    }

}
