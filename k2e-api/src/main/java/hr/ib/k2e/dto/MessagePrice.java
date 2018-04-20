package hr.ib.k2e.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MessagePrice {
    String id;
    String accountId;
    String messageLogId;
    Integer price;
    boolean isFinal;
}
