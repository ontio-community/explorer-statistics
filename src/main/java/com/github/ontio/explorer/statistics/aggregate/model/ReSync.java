package com.github.ontio.explorer.statistics.aggregate.model;

import com.github.ontio.explorer.statistics.model.Contract;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author LiuQi
 */
@RequiredArgsConstructor
@Getter
public class ReSync implements Serializable {

	private static final Lock RE_SYNC_LOCK = new ReentrantLock();

	private static final Condition RE_SYNC_BEGUN = RE_SYNC_LOCK.newCondition();

	private static final Condition RE_SYNC_ENDED = RE_SYNC_LOCK.newCondition();

	private final String contractHash;

	private final int fromBlock;

	private final int toBlock;

	public boolean waitToBegin(long time, TimeUnit unit) throws InterruptedException {
		RE_SYNC_LOCK.lock();
		try {
			return RE_SYNC_BEGUN.await(time, unit);
		} finally {
			RE_SYNC_LOCK.unlock();
		}
	}

	public void readyToBegin() {
		RE_SYNC_LOCK.lock();
		try {
			RE_SYNC_BEGUN.signal();
		} finally {
			RE_SYNC_LOCK.unlock();
		}
	}
	
	public boolean waitToEnd(long time, TimeUnit unit) throws InterruptedException {
		RE_SYNC_LOCK.lock();
		try {
			return RE_SYNC_ENDED.await(time, unit);
		} finally {
			RE_SYNC_LOCK.unlock();
		}
	}
	
	public void readyToEnd() {
		RE_SYNC_LOCK.lock();
		try {
			RE_SYNC_ENDED.signal();
		} finally {
			RE_SYNC_LOCK.unlock();
		}
	}

	public Begin begin() {
		return new Begin(this);
	}

	public End end() {
		return new End(this);
	}
	
	public Contract contractForUpdate() {
		Contract contract = new Contract();
		contract.setContractHash(contractHash);
		return contract;
	}

	@RequiredArgsConstructor
	@Getter
	public static class Begin implements Serializable {
		private final ReSync reSync;
	}

	@RequiredArgsConstructor
	@Getter
	public static class End implements Serializable {
		private final ReSync reSync;
	}

}
