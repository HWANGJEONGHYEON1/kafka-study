package com.example.processor;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

// 스트림 프로세서를 사용하려면 kafka-streams 라이브러리에서 제공해주는 Processor, Transformer 인터페이스를 사용해야한다.
public class FilterProcessor implements Processor<String, String> {

    // 프로세서에 대한 정보를 담고 있다. 현재 스트림 처리중인 토폴로지의 토픽정보, 애플리케이션 id,를 조회할 수 있다.
    // schedule(), forward, commit() 등의 프로세싱 처리에 필요한 메서드를 사용할 수 있다.
    private ProcessorContext context;

    // 초기화
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    // 실제 로직이 들어간다.
    @Override
    public void process(String key, String value) {
        if (value.length() > 5) {
            context.forward(key, value);
        }

        context.commit();
    }

    @Override
    public void close() {

    }
}
