package hr.ib.k2e.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ESMessage {
    MessageLog messageLog;
    MessagePrice messagePrice;
}
