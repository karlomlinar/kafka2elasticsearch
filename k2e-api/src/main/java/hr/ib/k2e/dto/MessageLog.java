package hr.ib.k2e.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MessageLog {
    String id;
    Integer networkId;
    String status;
    boolean isFinal;
    LocalDate date;
}
