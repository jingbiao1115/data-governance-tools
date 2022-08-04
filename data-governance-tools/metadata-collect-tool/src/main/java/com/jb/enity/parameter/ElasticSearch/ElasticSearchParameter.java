package com.jb.enity.parameter.ElasticSearch;

import com.jb.enity.parameter.IParameter;
import lombok.Data;

import javax.validation.constraints.NotBlank;

/**
 * @author zhaojb
 */
@Data
public class ElasticSearchParameter implements IParameter {
    /**
     * host
     */
    @NotBlank(message = "host不能为空")
    private String host;

    private String username;

    private String password;
}
