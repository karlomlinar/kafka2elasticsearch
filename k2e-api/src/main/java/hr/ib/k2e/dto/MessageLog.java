package hr.ib.k2e.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MessageLog {
    String id;
    Integer networkId;
    String status;
    boolean isFinal;
    long date;
}
