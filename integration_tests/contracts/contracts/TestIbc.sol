// SPDX-License-Identifier: MIT
pragma solidity ^0.6.6;

contract TestIbc {
    address constant ibcContract = 0x0000000000000000000000000000000000000065;
    uint256 lastAckSeq;
    
    function nativeTransfer(string memory portId, string memory channelId, address sender, string memory receiver, uint256 amount, string memory srcDenom, string memory dstDenom, uint256 ratio, uint256 timeout) public returns (uint256) {
        (bool result, bytes memory data) = ibcContract.call(abi.encodeWithSignature(
            "transfer(string,string,address,string,uint256,string,string,uint256,uint256)", portId, channelId, sender, receiver, amount, srcDenom, dstDenom, ratio, timeout
        ));
        require(result, "native call");
        lastAckSeq = abi.decode(data, (uint256));
        return lastAckSeq;
    }
    function nativeHasCommit(string memory portId, string memory channelId, uint256 seq) public returns (bool) {
       (bool result, bytes memory data) = ibcContract.call(abi.encodeWithSignature(
            "hasCommit(string,string,uint256)", portId, channelId, seq
        ));
        require(result, "native call");
        return abi.decode(data, (bool));
    }
    function nativeQueryNextSeq(string memory portId, string memory channelId) public returns (uint256) {
       (bool result, bytes memory data) = ibcContract.call(abi.encodeWithSignature(
            "queryNextSeq(string,string)", portId, channelId
        ));
        require(result, "native call");
        return abi.decode(data, (uint256));
    }
    function getLastAckSeq() public returns (uint256) {
        return lastAckSeq;
    }
     function nativeTransferRevert(string memory portId, string memory channelId, address sender, string memory receiver, uint256 amount, string memory srcDenom, string memory dstDenom, uint256 ratio, uint256 timeout) public {
        nativeTransfer(portId, channelId, sender, receiver, amount, srcDenom, dstDenom, ratio, timeout);
        revert("test");
    }
}