/*
 * Copyright (C) 2018 The ontology Authors
 * This file is part of The ontology library.
 * The ontology is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * The ontology is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License
 * along with The ontology.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.github.ontio.explorer.statistics.task;

import com.github.ontio.explorer.statistics.service.StatisticsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@EnableScheduling
public class DailyInfoSchedule {

    private final StatisticsService statisticsService;

    @Autowired
    public DailyInfoSchedule(StatisticsService statisticsService) {
        this.statisticsService = statisticsService;
    }

    @Scheduled(cron = "0 5 0 * * *")
    public void updateDailyInfo() {
        log.info("Updating daily information task begin");
        statisticsService.updateDailySummary();
        log.info("Updating daily information task end");
    }

    @Scheduled(cron = "0 0/30 * * * *")
    public void updateApprovedContractInfo() {
        log.info("Updating approved contract information task begin");
        statisticsService.updateApprovedContractInfo();
        log.info("Updating approved contract information task end");
    }

    @Scheduled(fixedDelayString = "${node-schedule-task.update-node-annualized-yield}")
    public void updateTotalOngSupply() {
        try {
            log.info("Updating total ong supply task begin");
            statisticsService.updateTotalOngSupply();
            log.info("Updating total ong supply task end");
        } catch (Exception e) {
            log.warn("Updating total ong supply failed: {}", e.getMessage());
        }
    }
}
