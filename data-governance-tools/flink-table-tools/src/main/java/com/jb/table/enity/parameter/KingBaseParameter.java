package com.jb.table.enity.parameter;

import com.jb.table.enity.IParameter;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import javax.validation.constraints.NotBlank;

/**
 * @author zhaojb
 * KingBase请求参数
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class KingBaseParameter extends CommonParameter implements IParameter {

    @NotBlank(message = "数据库不能为空")
    private String catalog;

    private String schema;
}
