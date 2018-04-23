package hr.ib.k2e.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ESMessage {
    String id;
    Integer networkId;
    String status;
    boolean isFinal;
    long date;;
    List<MessagePrice> billing;
}
