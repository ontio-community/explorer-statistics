package com.github.ontio.explorer.statistics.controller;

import com.github.ontio.explorer.statistics.common.Response;
import com.github.ontio.explorer.statistics.common.Result;
import com.github.ontio.explorer.statistics.model.dto.UpdateOffChainNodeInfoDto;
import com.github.ontio.explorer.statistics.service.ConfigService;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.*;

@CrossOrigin
@RestController
@RequestMapping("v2/nodes/")
public class ConfigController {

    private ConfigService configService;

    public ConfigController(ConfigService configService) {
        this.configService = configService;
    }

    @ApiOperation(value = "Get max staking change count from DB")
    @GetMapping(value = "/max-staking-change-count")
    public Response getMaxStakingChangeCount() {
        String count = configService.getMaxStakingChangeCount();
        if (count == null || count.length() == 0) {
            return new Response(Result.INTERNAL_SERVER_ERROR);
        }
        return new Response(Result.SUCCESS, count);
    }

    @ApiOperation(value = "Update max staking change count from Blockchain")
    @PutMapping(value = "/max-staking-change-count")
    public Response refreshBlockCountInStakingRound() {
        String count = configService.updateMaxStakingChangeCount();
        if (count == null || count.length() == 0) {
            return new Response(Result.INTERNAL_SERVER_ERROR);
        }
        return new Response(Result.SUCCESS, count);
    }

    @ApiOperation(value = "insert or update node register information by public key")
    @PostMapping(value = "/off-chain-info")
    public Response updateOffChainInfoByPublicKey(@RequestBody UpdateOffChainNodeInfoDto updateOffChainNodeInfoDto) throws Exception {
        Response response = configService.updateOffChainInfoByPublicKey(updateOffChainNodeInfoDto);
        return response;
    }


}
